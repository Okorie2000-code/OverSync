import { createPublicClient, http, parseAbiItem, type PublicClient } from "viem";
import { sepolia, mainnet } from "viem/chains";
import type { Logger } from "pino";
import type { CoordinatorConfig } from "../config.js";
import type { OrderService } from "../services/order-service.js";
import { listenerLastBlock } from "../metrics.js";
import type { ChainEventProcessor, SettlementEvent } from "../services/chain-events.js";

const ORDER_CREATED = parseAbiItem(
  "event OrderCreated(uint256 indexed orderId, address indexed sender, address indexed beneficiary, address token, uint256 amount, uint256 safetyDeposit, bytes32 hashlock, uint64 timelock)"
);
const ORDER_CLAIMED = parseAbiItem(
  "event OrderClaimed(uint256 indexed orderId, address indexed claimer, bytes32 preimage, uint256 amount, uint256 safetyDeposit)"
);
const ORDER_REFUNDED = parseAbiItem(
  "event OrderRefunded(uint256 indexed orderId, address indexed caller, uint256 amount, uint256 safetyDeposit)"
);

export class EthereumListener {
  private readonly client: PublicClient;
  private readonly log: Logger;
  private unwatchers: Array<() => void> = [];

  constructor(
    private readonly cfg: CoordinatorConfig,
    private readonly orders: OrderService,
    log: Logger,
    /** When supplied, claims/refunds are applied exactly once and the block cursor is persisted. */
    private readonly events?: ChainEventProcessor
  ) {
    this.log = log.child({ component: "EthereumListener" });
    this.client = createPublicClient({
      chain: cfg.ethereum.chainId === 1 ? mainnet : sepolia,
      transport: http(cfg.ethereum.rpcUrl)
    });
  }

  private get networkId(): string {
    return `ethereum:${this.cfg.ethereum.chainId}`;
  }

  /**
   * Resume from the persisted block cursor: re-read logs from the last
   * fully processed block (inclusive; the processor skips events it has
   * already applied) up to the chain head, then live-watch. Throws
   * CursorMismatchError when the saved cursor belongs to another network.
   * With no saved cursor the listener starts at the current head.
   */
  async start(): Promise<void> {
    if (!this.cfg.ethereum.htlcEscrow) {
      this.log.warn("ETH_HTLC_ESCROW not configured - Ethereum listener disabled");
      return;
    }
    const address = this.cfg.ethereum.htlcEscrow;
    this.log.info({ contract: address }, "starting");

    if (this.events) {
      const saved = await this.events.resume("ethereum", this.networkId);
      const head = await this.client.getBlockNumber();
      if (saved) {
        await this.catchUp(address, BigInt(saved.position), head);
      } else {
        await this.events.advance("ethereum", this.networkId, Number(head));
      }
    }

    this.unwatchers.push(
      this.client.watchEvent({
        address,
        event: ORDER_CREATED,
        onLogs: (logs) => {
          void (async () => {
            for (const log of logs) {
              if (log.blockNumber != null) {
                listenerLastBlock.set({ chain: "ethereum" }, Number(log.blockNumber));
              }
              await this.handleCreated(log);
            }
          })();
        }
      })
    );

    this.unwatchers.push(
      this.client.watchEvent({
        address,
        event: ORDER_CLAIMED,
        onLogs: (logs) => {
          for (const log of logs) {
            if (log.blockNumber != null) {
              listenerLastBlock.set({ chain: "ethereum" }, Number(log.blockNumber));
            }
            this.log.info({ orderId: log.args.orderId!.toString() }, "ETH order claimed");
            void this.applySettlement(toSettlement(log, "claimed"));
          }
        }
      })
    );

    this.unwatchers.push(
      this.client.watchEvent({
        address,
        event: ORDER_REFUNDED,
        onLogs: (logs) => {
          for (const log of logs) {
            if (log.blockNumber != null) {
              listenerLastBlock.set({ chain: "ethereum" }, Number(log.blockNumber));
            }
            this.log.info({ orderId: log.args.orderId!.toString() }, "ETH order refunded");
            void this.applySettlement(toSettlement(log, "refunded"));
          }
        }
      })
    );
  }

  private async handleCreated(log: {
    args: { hashlock?: `0x${string}`; orderId?: bigint; timelock?: bigint };
    transactionHash: string;
    blockNumber: bigint | null;
  }): Promise<void> {
    const hashlock = log.args.hashlock!;
    try {
      const order = await this.orders.findByHashlock(hashlock);
      if (!order) {
        this.log.info(
          { hashlock, orderId: log.args.orderId?.toString() },
          "ETH order observed without local announce"
        );
        return;
      }
      await this.orders.recordSrcLock({
        publicId: order.publicId,
        orderId: log.args.orderId!.toString(),
        txHash: log.transactionHash,
        blockNumber: Number(log.blockNumber),
        timelock: Number(log.args.timelock!)
      });
    } catch (err) {
      this.log.warn({ err, hashlock }, "could not record src lock");
    }
  }

  /** Live path: apply one settlement event and move the cursor to its block. */
  private async applySettlement(ev: SettlementEvent): Promise<void> {
    if (!this.events) return;
    try {
      await this.events.processBatch(this.networkId, "ethereum", [ev], ev.position);
    } catch (err) {
      // The cursor stays behind this event, so it is redelivered on restart.
      this.log.error({ err, txHash: ev.txHash }, "could not apply settlement event");
    }
  }

  private async catchUp(address: `0x${string}`, from: bigint, to: bigint): Promise<void> {
    if (!this.events || from > to) return;
    this.log.info({ from: from.toString(), to: to.toString() }, "resuming from saved cursor");
    const [created, claimed, refunded] = await Promise.all([
      this.client.getLogs({ address, event: ORDER_CREATED, fromBlock: from, toBlock: to }),
      this.client.getLogs({ address, event: ORDER_CLAIMED, fromBlock: from, toBlock: to }),
      this.client.getLogs({ address, event: ORDER_REFUNDED, fromBlock: from, toBlock: to })
    ]);
    for (const log of created) await this.handleCreated(log);
    await this.events.processBatch(
      this.networkId,
      "ethereum",
      [
        ...claimed.map((l) => toSettlement(l, "claimed")),
        ...refunded.map((l) => toSettlement(l, "refunded"))
      ],
      Number(to)
    );
  }

  stop(): void {
    for (const u of this.unwatchers) u();
    this.unwatchers = [];
  }
}

function toSettlement(
  log: {
    args: { orderId?: bigint; preimage?: `0x${string}` };
    transactionHash: string;
    logIndex: number | null;
    blockNumber: bigint | null;
  },
  kind: "claimed" | "refunded"
): SettlementEvent {
  return {
    chain: "ethereum",
    kind,
    onchainOrderId: log.args.orderId!.toString(),
    txHash: log.transactionHash,
    logIndex: log.logIndex ?? 0,
    position: Number(log.blockNumber),
    preimage: kind === "claimed" ? log.args.preimage : undefined
  };
}
