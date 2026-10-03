package relengine

import (
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
	return &MemPool{total: total}
}

// Acquire 申请至多 n 行额度，返回实际授予的行数（可能小于 n，甚至为 0）。
// 调用方必须只在内存中保留不超过授予量的行，其余转入临时存储。
func (p *MemPool) Acquire(n int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
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

// Release 归还 n 行额度（不超过已授予量）。取消、失败或正常结束
// 都必须调用，保证同时进行的独立阶段能继续使用额度。
func (p *MemPool) Release(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.used -= n
	if p.used < 0 {
		p.used = 0
	}
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
