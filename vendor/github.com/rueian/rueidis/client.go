package rueidis

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rueian/rueidis/internal/cmds"
)

type singleClient struct {
	conn  conn
	stop  uint32
	cmd   cmds.Builder
	retry bool
}

func newSingleClient(opt *ClientOption, prev conn, connFn connFn) (*singleClient, error) {
	if len(opt.InitAddress) == 0 {
		return nil, ErrNoAddr
	}

	conn := connFn(opt.InitAddress[0], opt)
	conn.Override(prev)
	if err := conn.Dial(); err != nil {
		return nil, err
	}
	return newSingleClientWithConn(conn, cmds.NewBuilder(cmds.NoSlot), !opt.DisableRetry), nil
}

func newSingleClientWithConn(conn conn, builder cmds.Builder, retry bool) *singleClient {
	return &singleClient{cmd: builder, conn: conn, retry: retry}
}

func (c *singleClient) B() cmds.Builder {
	return c.cmd
}

func (c *singleClient) Do(ctx context.Context, cmd cmds.Completed) (resp RedisResult) {
retry:
	resp = c.conn.Do(ctx, cmd)
	if c.retry && cmd.IsReadOnly() && c.isRetryable(resp.NonRedisError(), ctx) {
		goto retry
	}
	if resp.NonRedisError() == nil { // not recycle cmds if error, since cmds may be used later in pipe. consider recycle them by pipe
		cmds.Put(cmd.CommandSlice())
	}
	return resp
}

func (c *singleClient) DoMulti(ctx context.Context, multi ...cmds.Completed) (resps []RedisResult) {
	if len(multi) == 0 {
		return nil
	}
retry:
	resps = c.conn.DoMulti(ctx, multi...)
	if c.retry && allReadOnly(multi) {
		for _, resp := range resps {
			if c.isRetryable(resp.NonRedisError(), ctx) {
				goto retry
			}
		}
	}
	for i, cmd := range multi {
		if resps[i].NonRedisError() == nil {
			cmds.Put(cmd.CommandSlice())
		}
	}
	return resps
}

func (c *singleClient) DoMultiCache(ctx context.Context, multi ...CacheableTTL) (resps []RedisResult) {
	if len(multi) == 0 {
		return nil
	}
retry:
	resps = c.conn.DoMultiCache(ctx, multi...)
	if c.retry {
		for _, resp := range resps {
			if c.isRetryable(resp.NonRedisError(), ctx) {
				goto retry
			}
		}
	}
	for i, cmd := range multi {
		if err := resps[i].NonRedisError(); err == nil || err == ErrDoCacheAborted {
			cmds.Put(cmd.Cmd.CommandSlice())
		}
	}
	return resps
}

func (c *singleClient) DoCache(ctx context.Context, cmd cmds.Cacheable, ttl time.Duration) (resp RedisResult) {
retry:
	resp = c.conn.DoCache(ctx, cmd, ttl)
	if c.retry && c.isRetryable(resp.NonRedisError(), ctx) {
		goto retry
	}
	if err := resp.NonRedisError(); err == nil || err == ErrDoCacheAborted {
		cmds.Put(cmd.CommandSlice())
	}
	return resp
}

func (c *singleClient) Receive(ctx context.Context, subscribe cmds.Completed, fn func(msg PubSubMessage)) (err error) {
retry:
	err = c.conn.Receive(ctx, subscribe, fn)
	if c.retry {
		if _, ok := err.(*RedisError); !ok && c.isRetryable(err, ctx) {
			goto retry
		}
	}
	if err == nil {
		cmds.Put(subscribe.CommandSlice())
	}
	return err
}

func (c *singleClient) Dedicated(fn func(DedicatedClient) error) (err error) {
	wire := c.conn.Acquire()
	dsc := &dedicatedSingleClient{cmd: c.cmd, conn: c.conn, wire: wire, retry: c.retry}
	err = fn(dsc)
	dsc.release()
	return err
}

func (c *singleClient) Dedicate() (DedicatedClient, func()) {
	wire := c.conn.Acquire()
	dsc := &dedicatedSingleClient{cmd: c.cmd, conn: c.conn, wire: wire, retry: c.retry}
	return dsc, dsc.release
}

func (c *singleClient) Nodes() map[string]Client {
	return map[string]Client{c.conn.Addr(): c}
}

func (c *singleClient) Close() {
	atomic.StoreUint32(&c.stop, 1)
	c.conn.Close()
}

type dedicatedSingleClient struct {
	conn conn
	wire wire
	mark uint32
	cmd  cmds.Builder

	retry bool
}

func (c *dedicatedSingleClient) B() cmds.Builder {
	return c.cmd
}

func (c *dedicatedSingleClient) Do(ctx context.Context, cmd cmds.Completed) (resp RedisResult) {
retry:
	resp = c.wire.Do(ctx, cmd)
	if c.retry && cmd.IsReadOnly() && isRetryable(resp.NonRedisError(), c.wire, ctx) {
		goto retry
	}
	if resp.NonRedisError() == nil {
		cmds.Put(cmd.CommandSlice())
	}
	return resp
}

func (c *dedicatedSingleClient) DoMulti(ctx context.Context, multi ...cmds.Completed) (resp []RedisResult) {
	if len(multi) == 0 {
		return nil
	}
	retryable := c.retry
	if retryable {
		retryable = allReadOnly(multi)
	}
retry:
	resp = c.wire.DoMulti(ctx, multi...)
	if retryable && anyRetryable(resp, c.wire, ctx) {
		goto retry
	}
	for i, cmd := range multi {
		if resp[i].NonRedisError() == nil {
			cmds.Put(cmd.CommandSlice())
		}
	}
	return resp
}

func (c *dedicatedSingleClient) Receive(ctx context.Context, subscribe cmds.Completed, fn func(msg PubSubMessage)) (err error) {
retry:
	err = c.wire.Receive(ctx, subscribe, fn)
	if c.retry {
		if _, ok := err.(*RedisError); !ok && isRetryable(err, c.wire, ctx) {
			goto retry
		}
	}
	if err == nil {
		cmds.Put(subscribe.CommandSlice())
	}
	return err
}

func (c *dedicatedSingleClient) SetPubSubHooks(hooks PubSubHooks) <-chan error {
	return c.wire.SetPubSubHooks(hooks)
}

func (c *dedicatedSingleClient) Close() {
	c.wire.Close()
	c.release()
}

func (c *dedicatedSingleClient) release() {
	if atomic.CompareAndSwapUint32(&c.mark, 0, 1) {
		c.conn.Store(c.wire)
	}
}

func (c *singleClient) isRetryable(err error, ctx context.Context) bool {
	return err != nil && atomic.LoadUint32(&c.stop) == 0 && ctx.Err() == nil
}

func isRetryable(err error, w wire, ctx context.Context) bool {
	return err != nil && w.Error() == nil && ctx.Err() == nil
}

func anyRetryable(resp []RedisResult, w wire, ctx context.Context) bool {
	for _, r := range resp {
		if isRetryable(r.NonRedisError(), w, ctx) {
			return true
		}
	}
	return false
}

func allReadOnly(multi []cmds.Completed) bool {
	for _, cmd := range multi {
		if cmd.IsWrite() {
			return false
		}
	}
	return true
}

func allSameSlot(multi []cmds.Completed) bool {
	for i := 1; i < len(multi); i++ {
		if multi[0].Slot() != multi[i].Slot() {
			return false
		}
	}
	return true
}
