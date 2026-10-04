package relengine

import (
	"context"
	"fmt"
	"sync"
)

// MemPool 是一条查询内所有算子共享的内存额度（单位：行）。
// 多个连接/聚合阶段从同一个池子申请额度：某个阶段占用较多时，
// 其余阶段只能拿到剩余部分，拿不到就转入临时存储（落盘），
// 而不是各自把整份预算重复计算。池子按查询独立创建，
// 互不影响同时进行的其他查询。
type MemPool struct {
	mu     sync.Mutex
	cond   *sync.Cond
	total  int
	used   int
	peak   int
	spills int
	events []string
}

// NewMemPool 创建总额度为 total 行的内存池。
func NewMemPool(total int) *MemPool {
	if total < 1 {
		total = 1
	}
	p := &MemPool{total: total}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Acquire 申请至多 n 行额度，返回实际授予的行数（可能小于 n，甚至为 0）。
// 调用方必须只在内存中保留不超过授予量的行，其余转入临时存储。
// 非阻塞：拿不到就返回 0，由调用方决定落盘或报预算不足。
func (p *MemPool) Acquire(n int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acquireLocked(n)
}

func (p *MemPool) acquireLocked(n int) int {
	grant := n
	if avail := p.total - p.used; grant > avail {
		grant = avail
	}
	if grant < 0 {
		grant = 0
	}
	p.used += grant
	if p.used > p.peak {
		p.peak = p.used
	}
	return grant
}

// AcquireWait 与 Acquire 类似，但当一行额度也拿不到时阻塞等待，
// 直到其他阶段/查询 Release 腾出额度或 ctx 取消。等待与获得都会
// 记入 Events，便于调用方核对“占用-等待-释放”的对应关系。
// 注意：单条顺序执行的查询内部不应使用它——额度只能由本查询自己
// 释放时等待会死锁；执行器在额度不足时返回 ErrInsufficientBudget。
func (p *MemPool) AcquireWait(ctx context.Context, n int) (int, error) {
	stop := context.AfterFunc(ctx, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.cond.Broadcast()
	})
	defer stop()
	p.mu.Lock()
	defer p.mu.Unlock()
	waited := false
	for {
		if grant := p.acquireLocked(n); grant > 0 {
			if waited {
				p.events = append(p.events,
					fmt.Sprintf("等待结束：获得 %d 行额度", grant))
			}
			return grant, nil
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if !waited {
			waited = true
			p.events = append(p.events,
				fmt.Sprintf("额度不足（需要至多 %d 行，剩余 0 行），等待其他执行释放", n))
		}
		p.cond.Wait()
	}
}

// Release 归还 n 行额度（不超过已授予量）。取消、失败或正常结束
// 都必须调用，保证同时进行的独立阶段能继续使用额度。
func (p *MemPool) Release(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.used -= n
	if p.used < 0 {
		p.used = 0
	}
	p.cond.Broadcast()
}

// NoteSpill 记录一次因额度不足转入临时存储的事件，说明等待/落盘原因。
func (p *MemPool) NoteSpill(format string, args ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.spills++
	p.events = append(p.events, fmt.Sprintf(format, args...))
}

// PoolStats 是内存池的快照：实际占用与历史峰值都如实上报。
type PoolStats struct {
	Total  int      // 总额度（行）
	InUse  int      // 当前已授予（行）
	Peak   int      // 历史峰值占用（行）
	Spills int      // 因额度不足落盘的次数
	Events []string // 每次落盘/等待的原因说明
}

// Stats 返回当前池状态快照。
func (p *MemPool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{
		Total:  p.total,
		InUse:  p.used,
		Peak:   p.peak,
		Spills: p.spills,
		Events: append([]string(nil), p.events...),
	}
}
