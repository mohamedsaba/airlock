/**
 * Calculates the next retry time in milliseconds using exponential backoff with full jitter.
 * Formula: delay = min(maxBackoffMs, baseMs * 2^retryCount)
 * Jitter: random() * delay
 * 
 * @param retryCount - Current number of retries
 * @param baseMs - Initial delay in milliseconds (default: 1000)
 * @param maxBackoffMs - Maximum delay in milliseconds (default: 300000 / 5 minutes)
 * @param useFullJitter - Whether to apply full jitter (default: true)
 * @returns Delay in milliseconds
 */
export function calculateNextRetryAt(
  retryCount: number,
  baseMs: number = 1000,
  maxBackoffMs: number = 300000,
  useFullJitter: boolean = true,
): number {
  const delay = Math.min(maxBackoffMs, baseMs * Math.pow(2, retryCount));

  if (!useFullJitter) {
    return delay;
  }

  return Math.random() * delay;
}
