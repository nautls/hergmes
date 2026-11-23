/** The codes a REST error carries. A relay newer than this SDK may send others. */
export type ErrorCode =
  | "invalid_address"
  | "invalid_token_id"
  | "invalid_template"
  | "invalid_ids"
  | "invalid_box_id"
  | "invalid_broadcast"
  | "transaction_rejected"
  | "invalid_cursor"
  | "invalid_limit"
  | "key_not_allowed"
  | "unknown_token"
  | "box_not_found"
  | "cursor_expired"
  | "supply_too_large"
  | "too_many_boxes"
  | "unsupported_template"
  | "capacity_exceeded"
  | "not_ready"
  | "node_unavailable"
  | "box_unavailable"
  | "node_error"
  | "internal_error";

/** Why the node refused a tx, on `transaction_rejected`. */
export type RejectReason =
  | "unparseable"
  | "too_large"
  | "invalid"
  | "double_spend"
  | "fee_too_low"
  | "already_in_pool"
  | "pool_full"
  | "policy";

/**
 * A response other than 2xx. `code` and `reason` are absent when the body is not the relay's
 * error, as with an unknown route or a proxy's own page. `retryAfter` is in seconds, from the
 * `Retry-After` header, and is set on every 503 the relay sends.
 */
export class RelayError extends Error {
  override readonly name = "RelayError";
  readonly status: number;
  readonly code: ErrorCode | (string & {}) | undefined;
  readonly reason: RejectReason | (string & {}) | undefined;
  readonly retryAfter: number | undefined;

  constructor(
    status: number,
    message: string,
    fields: { code?: string; reason?: string; retryAfter?: number } = {},
  ) {
    super(message);
    this.status = status;
    this.code = fields.code;
    this.reason = fields.reason;
    this.retryAfter = fields.retryAfter;
  }

  /** Reads a failed response: the relay's `{ error, reason?, message }`, or anything else. */
  static async from(response: Response): Promise<RelayError> {
    const retryAfter = seconds(response.headers.get("retry-after"));
    const text = await response.text().catch(() => "");
    const body = parse(text);
    if (body) {
      return new RelayError(response.status, body.message, {
        code: body.error,
        ...(body.reason === undefined ? {} : { reason: body.reason }),
        ...(retryAfter === undefined ? {} : { retryAfter }),
      });
    }
    const message = text.trim() === "" ? `HTTP ${String(response.status)}` : text.trim();
    return new RelayError(response.status, message, retryAfter === undefined ? {} : { retryAfter });
  }
}

interface ErrorBody {
  error: string;
  reason?: string;
  message: string;
}

function parse(text: string): ErrorBody | undefined {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return undefined;
  }
  if (typeof value !== "object" || value === null) return undefined;
  const { error, reason, message } = value as Record<string, unknown>;
  if (typeof error !== "string" || typeof message !== "string") return undefined;
  if (reason !== undefined && typeof reason !== "string") return undefined;
  return reason === undefined ? { error, message } : { error, reason, message };
}

/** `Retry-After` in seconds. The relay sends whole seconds; an HTTP date is read too. */
function seconds(header: string | null): number | undefined {
  if (header === null) return undefined;
  const trimmed = header.trim();
  if (/^\d+$/.test(trimmed)) return Number(trimmed);
  const date = Date.parse(trimmed);
  if (Number.isNaN(date)) return undefined;
  return Math.max(0, Math.ceil((date - Date.now()) / 1000));
}
