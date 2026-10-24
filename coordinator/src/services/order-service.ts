import { Address, Hash } from '../types';
import { ConfigService } from './config-service';

interface LockRequest {
  lockHash: Hash;
  target: Address;
  amount: bigint;
  expiration: number;
}

export class OrderService {
  private config: ConfigService;

  constructor(config: ConfigService) {
    this.config = config;
  }

  async buildLockOrder(request: LockRequest): Promise<any> {
    const activeV2Escrow = this.config.getActiveV2Escrow();

    if (activeV2Escrow && activeV2Escrow.toLowerCase() !== request.target.toLowerCase()) {
      throw new Error("Legacy bridge lock rejected: v2 escrow active");
    }

    // Proceed with v2 escrow order building
    return this.buildV2EscrowOrder(request);
  }

  private async buildV2EscrowOrder(request: LockRequest): Promise<any> {
    // Implementation for v2 escrow order building
    return {
      type: 'v2_escrow_lock',
      ...request
    };
  }
}

export class LegacyLockError extends Error {
  constructor() {
    super("legacy lock refused");
    this.name = "LegacyLockError";
  }
}

/** Use the v2 escrow when it is configured. A legacy-bridge target builds nothing. */
export function resolveLockTarget(input: {
  v2Escrow?: string | null;
  requestedTarget: string;
  legacyBridge: string;
}): { target: string } {
  const v2 = (input.v2Escrow ?? "").trim();
  if (!v2) return { target: input.requestedTarget };
  if (input.requestedTarget.toLowerCase() === input.legacyBridge.toLowerCase()) {
    throw new LegacyLockError();
  }
  return { target: v2 };
}