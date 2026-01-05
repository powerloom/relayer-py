# Redis Key Expiry and Memory Leak Fixes

## Summary

This document details the fixes applied to address:
1. **Redis keys missing TTL** - causing unbounded memory growth in Redis
2. **Memory leaks in relayer-py** - causing unbounded memory growth in the Python process

## Issues Found

### 1. Redis Keys Missing TTL

#### Issue: `epoch_batch_size:{epochId}` - No TTL
- **Location**: `relayer.py:225-228`
- **Impact**: Keys accumulate indefinitely in Redis, consuming memory
- **Fix**: Added 24-hour TTL (covers epoch lifecycle)

#### Issue: `epoch_batch_submissions:{epochId}` - No TTL on SET
- **Location**: `tx_worker.py:667-670`
- **Impact**: SET grows indefinitely as submissions are tracked, consuming memory
- **Fix**: Added 24-hour TTL with check to avoid resetting existing expiry

### 2. Memory Leaks in relayer-py

#### Issue: `TransactionQueue._tx_results` dict grows indefinitely
- **Location**: `utils/tx_queue.py:59`
- **Impact**: Memory usage grows unbounded as transaction results accumulate
- **Fix**: Added cleanup task that runs every 5 minutes, keeping only the most recent 1000 transactions

#### Issue: `TransactionQueue._pending_txs` dict grows indefinitely
- **Location**: `utils/tx_queue.py:58`
- **Impact**: Memory usage grows unbounded as pending transaction futures accumulate
- **Fix**: Cleanup task removes completed futures from the dict

## Fixes Applied

### 1. Added TTL to `epoch_batch_size` key

**File**: `relayer.py`

```python
# Before
await request.app.state.writer_redis_pool.set(
    epoch_batch_size(req_parsed.epochID),
    req_parsed.batchSize,
)

# After
await request.app.state.writer_redis_pool.set(
    epoch_batch_size(req_parsed.epochID),
    req_parsed.batchSize,
    ex=86400,  # 24 hours TTL
)
```

### 2. Added TTL to `epoch_batch_submissions` SET

**File**: `tx_worker.py`

```python
# Before
await self.writer_redis_pool.sadd(
    epoch_batch_submissions(msg_obj.epochID),
    tx_id,
)

# After
submissions_key = epoch_batch_submissions(msg_obj.epochID)
await self.writer_redis_pool.sadd(
    submissions_key,
    tx_id,
)
# Set TTL only if key doesn't already have one
ttl = await self.writer_redis_pool.ttl(submissions_key)
if ttl == -1:  # Key exists but has no TTL
    await self.writer_redis_pool.expire(submissions_key, 86400)  # 24 hours
```

### 3. Added Memory Cleanup for Transaction Queue

**File**: `utils/tx_queue.py`

**Changes**:
1. Added `max_results_cache` parameter (default: 1000) to limit cache size
2. Added `_cleanup_task` background task that runs every 5 minutes
3. Cleanup removes:
   - Completed futures from `_pending_txs`
   - Old entries from `_tx_results` (keeps only most recent N)

**Cleanup Method**:
```python
async def _cleanup_old_results(self):
    """
    Background task to clean up old transaction results and pending futures.
    Runs every 5 minutes, keeps only the most recent transactions.
    """
    while self._processing:
        await asyncio.sleep(300)  # 5 minutes
        
        # Clean up completed futures
        completed_txs = [
            tx_id for tx_id, future in self._pending_txs.items()
            if future.done()
        ]
        for tx_id in completed_txs:
            del self._pending_txs[tx_id]
        
        # Clean up old results if cache too large
        if len(self._tx_results) > self._max_results_cache:
            # Keep only most recent transactions
            sorted_tx_ids = sorted(
                self._tx_results.keys(),
                key=lambda x: int(x.split('_')[1]) if '_' in x else 0,
                reverse=True
            )
            to_remove = sorted_tx_ids[self._max_results_cache:]
            for tx_id in to_remove:
                del self._tx_results[tx_id]
```

## Redis Connection Management

**Status**: ✅ Already properly handled

The `get_writer_redis_conn()` function is only used in `tx_queue.py` and connections are properly closed in a `finally` block:

```python
redis_conn = await get_writer_redis_conn()
try:
    # ... operations ...
finally:
    await redis_conn.close()
```

## Verification

### Redis Keys TTL Check

To verify all keys have proper TTL, run:

```bash
# Connect to Redis (port 6380 on localhost)
redis-cli -p 6380

# Check TTL for specific keys
TTL epoch_batch_size:<epochId>
TTL epoch_batch_submissions:<epochId>
TTL end_batch_submission_called:<dataMarket>:<epochId>

# Expected: All should return positive TTL values (in seconds)
```

### Memory Monitoring

Monitor relayer-py memory usage:

```bash
# Docker stats
docker stats dsv-relayer-py --no-stream --format "{{.MemUsage}}"

# Or inside container
ps aux | grep python
```

Expected behavior:
- Memory should stabilize after cleanup cycles
- Should not grow unbounded over time
- Old transaction results should be cleaned up every 5 minutes

## Additional Recommendations

### 1. Redis Key Audit

Consider auditing all Redis keys created by relayer-py to ensure they all have appropriate TTLs:

**Keys created by relayer-py**:
- `epoch_batch_size:{epochId}` ✅ Fixed (24h TTL)
- `epoch_batch_submissions:{epochId}` ✅ Fixed (24h TTL)
- `end_batch_submission_called:{dataMarket}:{epochId}` ✅ Already has TTL (1h)
- `{protocol}:{market}:epoch:{epochId}:state` ✅ Already has TTL (7 days)

### 2. Monitoring

Add monitoring/alerts for:
- Redis memory usage (should stabilize)
- relayer-py memory usage (should stabilize)
- Number of keys without TTL (should be minimal)
- Transaction queue cache size (should stay under limit)

### 3. Configuration

The `max_results_cache` parameter can be adjusted based on needs:
- Default: 1000 transactions
- Increase if you need longer transaction history
- Decrease if memory is constrained

## Testing

After deploying these fixes:

1. **Monitor Redis memory**: Should stabilize, not grow unbounded
2. **Monitor relayer-py memory**: Should stabilize after cleanup cycles
3. **Verify TTLs**: All new keys should have TTL set
4. **Verify cleanup**: Transaction queue cache should stay under limit

## Related Documentation

- `decentralized-sequencer/docs/REDIS_KEYS.md` - Complete Redis key architecture
- `decentralized-sequencer/docs/MONITORING_ARCHITECTURE.md` - Monitoring system design

