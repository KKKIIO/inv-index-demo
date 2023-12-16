package store

import (
	"context"
	"fmt"
	"strconv"

	"github.com/RoaringBitmap/roaring"
	"github.com/redis/go-redis/v9"
)

type RedisBmStore struct {
	RDB    *redis.Client
	Prefix string
}

func (s *RedisBmStore) Get(indexKey string, valueKey string) (*roaring.Bitmap, error) {
	hashKey := s.Prefix + indexKey
	value, err := s.RDB.HGet(context.Background(), hashKey, valueKey).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("HGET failed, hashKey=%s, valueKey=%s, err: %w", hashKey, valueKey, err)
	}
	return parseBitmap(value)
}

func (s *RedisBmStore) Set(indexKey string, valueKey string, value *roaring.Bitmap) error {
	hashKey := s.Prefix + indexKey
	// delete empty bitmaps, update non-empty bitmaps
	if value == nil || value.GetCardinality() == 0 {
		return s.RDB.HDel(context.Background(), hashKey, valueKey).Err()
	}
	raw, err := value.ToBytes()
	if err != nil {
		return err
	}
	return s.RDB.HSet(context.Background(), hashKey, valueKey, raw).Err()
}

// RedisSortKeyBitmapStore store sorted bitmaps in redis
// Value keys are stored in a sorted set, and bitmaps are stored in a hash
// numberic key is serialized as zero-padded hex string
type RedisSortKeyBitmapStore struct {
	RDB    *redis.Client
	Prefix string
}

func (s *RedisSortKeyBitmapStore) Scan(indexKey string, start uint64, stop uint64, reverse bool, limit int) ([]SortKeyBitmap, error) {
	zsetKey := s.makeZsetKey(indexKey)
	sstart := u64ToHex(start)
	sstop := u64ToHex(stop)
	args := redis.ZRangeArgs{
		Key:   zsetKey,
		ByLex: true,
		Rev:   reverse,
		Count: int64(limit),
	}
	if !reverse {
		args.Start = "[" + sstart
		args.Stop = "[" + sstop
	} else {
		args.Start = "[" + sstop
		args.Stop = "[" + sstart
	}

	keys, err := s.RDB.ZRangeArgs(context.Background(), args).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("ZRange failed, args=%+v, err: %w", args, err)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	hashKey := s.makeHashKey(indexKey)
	values, err := s.RDB.HMGet(context.Background(), hashKey, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("HMGet failed, hashKey=%s, keys=%+v, err: %w", hashKey, keys, err)
	}
	result := make([]SortKeyBitmap, len(keys))
	for i, key := range keys {
		sortKey, err := hexToU64(key)
		if err != nil {
			return nil, err
		}
		value, _ := values[i].(string)
		bm, err := parseBitmap(value)
		if err != nil {
			return nil, err
		}
		result[i] = SortKeyBitmap{SortKey: sortKey, Bitmap: bm}
	}
	return result, nil

}

func (s *RedisSortKeyBitmapStore) MSet(indexKey string, skbms []SortKeyBitmap) error {
	if len(skbms) == 0 {
		return nil
	}
	// delete empty bitmaps, update non-empty bitmaps
	zsetKey := s.makeZsetKey(indexKey)
	hashKey := s.makeHashKey(indexKey)
	delKeys := make([]uint64, 0)
	setSkbms := make([]SortKeyBitmap, 0)
	for _, skbm := range skbms {
		if skbm.Bitmap == nil || skbm.Bitmap.GetCardinality() == 0 {
			delKeys = append(delKeys, skbm.SortKey)
		} else {
			setSkbms = append(setSkbms, skbm)
		}
	}
	if len(delKeys) > 0 {
		members := make([]any, len(delKeys))
		fields := make([]string, len(delKeys))
		for i, key := range delKeys {
			fields[i] = u64ToHex(key)
			members[i] = fields[i]
		}
		if err := s.RDB.ZRem(context.Background(), zsetKey, members...).Err(); err != nil {
			return fmt.Errorf("ZRem failed, zsetKey=%s, members=%+v, err: %w", zsetKey, members, err)
		}
		if err := s.RDB.HDel(context.Background(), hashKey, fields...).Err(); err != nil {
			return fmt.Errorf("HDel failed, hashKey=%s, fields=%+v, err: %w", hashKey, fields, err)
		}
	}
	if len(setSkbms) > 0 {
		zs := make([]redis.Z, len(setSkbms))
		pairs := make([]any, len(setSkbms)*2)
		for i, skbm := range setSkbms {
			zs[i] = redis.Z{Score: float64(skbm.SortKey), Member: u64ToHex(skbm.SortKey)}
			pairs[i*2] = u64ToHex(skbm.SortKey)
			raw, err := skbm.Bitmap.ToBytes()
			if err != nil {
				return err
			}
			pairs[i*2+1] = raw
		}
		if err := s.RDB.ZAdd(context.Background(), zsetKey, zs...).Err(); err != nil {
			return fmt.Errorf("ZAdd failed, zsetKey=%s, zs=%+v, err: %w", zsetKey, zs, err)
		}
		if err := s.RDB.HMSet(context.Background(), hashKey, pairs...).Err(); err != nil {
			return fmt.Errorf("HMSet failed, hashKey=%s, pairs=%+v, err: %w", hashKey, pairs, err)
		}
	}
	return nil
}

func (s *RedisSortKeyBitmapStore) makeZsetKey(indexKey string) string {
	return s.Prefix + indexKey + ":zs"
}

func (s *RedisSortKeyBitmapStore) makeHashKey(indexKey string) string {
	return s.Prefix + indexKey + ":hm"
}

func u64ToHex(u uint64) string {
	return fmt.Sprintf("%016x", u)
}

func hexToU64(s string) (uint64, error) {
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("Failed to parse uint64, s=%s, err: %w", s, err)
	}
	return v, nil
}

type RedisFvStore struct {
	RDB    *redis.Client
	Prefix string
}

func (s *RedisFvStore) MGet(indexKey string, ids []uint32) ([]uint64, error) {
	hashKey := s.Prefix + indexKey
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = fmt.Sprint(id)
	}
	values, err := s.RDB.HMGet(context.Background(), hashKey, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("HMGet failed, hashKey=%s, keys=%+v, err: %w", hashKey, keys, err)
	}
	result := make([]uint64, len(values))
	for i, value := range values {
		var res uint64
		if sv, ok := value.(string); ok {
			if res, err = strconv.ParseUint(sv, 10, 64); err != nil {
				return nil, fmt.Errorf("Failed to parse uint64, hashKey=%s, key=%s, value=%s, err: %w", hashKey, keys[i], sv, err)
			}
		}
		result[i] = res
	}
	return result, nil
}

func (s *RedisFvStore) Set(indexKey string, id uint32, value uint64) error {
	hashKey := s.Prefix + indexKey
	return s.RDB.HSet(context.Background(), hashKey, fmt.Sprint(id), fmt.Sprint(value)).Err()
}
func (s *RedisFvStore) Remove(indexKey string, id uint32) error {
	hashKey := s.Prefix + indexKey
	return s.RDB.HDel(context.Background(), hashKey, fmt.Sprint(id)).Err()
}

type SortKeyBitmap struct {
	SortKey uint64
	Bitmap  *roaring.Bitmap
}

func parseBitmap(sv string) (*roaring.Bitmap, error) {
	roaringBitmap := roaring.New()
	if len(sv) == 0 {
		return roaringBitmap, nil
	}
	value := []byte(sv)
	if p, err := roaringBitmap.FromBuffer(value); err != nil {
		return nil, fmt.Errorf("Failed to decode bitmap: %w", err)
	} else if p != int64(len(value)) {
		return nil, fmt.Errorf("Corrupted bitmap data: p=%d, len(value)=%d", p, len(value))
	}
	return roaringBitmap, nil
}
