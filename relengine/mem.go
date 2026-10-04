package relengine

import (
	"context"
	"sync"
)

// MemManager 是一份可被多个算子（甚至多个查询）共享的内存额度池，
// 以“行”为单位记账。同一查询里的多个连接共享同一个池时，
// 某个阶段临时多占的额度会对其他阶段可见：后者要么等待（Acquire），
// 要么转入临时存储（TryAcquire 失败后落盘），不会各自把整份额度重复计算。
type MemManager struct {
	mu     sync.Mutex
	ch     chan struct{} // 释放时关闭并重建，用于唤醒等待者
	cap    int
	used   int
	high   int
	waits  []WaitEvent
	spills []SpillEvent
}

// WaitEvent 记录一次“不得不等待额度”的事件及原因。
type WaitEvent struct {
	Owner  string // 等待方（算子/查询）标识
	Rows   int    // 等待的行数额度
	Reason string // 等待原因
}

// SpillEvent 记录一次“额度不足转入临时存储”的事件及原因。
type SpillEvent struct {
	Owner  string
	Rows   int // 转入临时存储时已收集的行数
	Reason string
}

// MemStats 是额度池在某时刻的快照，用于报告实际占用。
type MemStats struct {
	Capacity  int          // 总额度（行）
	InUse     int          // 当前被占用的行数
	HighWater int          // 历史峰值占用（行）
	Waits     []WaitEvent  // 发生的等待及原因
	Spills    []SpillEvent // 发生的落盘及原因
}

// NewMemManager 创建容量为 capacityRows 行的额度池。
func NewMemManager(capacityRows int) *MemManager {
	if capacityRows < 1 {
		capacityRows = 1
	}
	return &MemManager{cap: capacityRows, ch: make(chan struct{})}
}

// Grant 是一份已获得的额度，Release 后失效。重复 Release 安全。
type Grant struct {
	m    *MemManager
	n    int
	once sync.Once
}

// Release 归还额度；幂等，取消/局部失败路径可放心调用。
func (g *Grant) Release() {
	if g == nil {
		return
	}
	g.once.Do(func() {
		m := g.m
		m.mu.Lock()
		m.used -= g.n
		close(m.ch)
		m.ch = make(chan struct{})
		m.mu.Unlock()
	})
}

// TryAcquire 尝试立即获得 n 行额度，失败返回 nil（调用方应转入临时存储）。
func (m *MemManager) TryAcquire(n int, owner string) *Grant {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n > m.cap-m.used {
		return nil
	}
	m.used += n
	if m.used > m.high {
		m.high = m.used
	}
	return &Grant{m: m, n: n}
}

// Acquire 获得 n 行额度，不足时阻塞等待，直到他人释放或 ctx 取消。
// 每次真实发生等待都会记录一条 WaitEvent（含原因），用于解释等待。
func (m *MemManager) Acquire(ctx context.Context, n int, owner, reason string) (*Grant, error) {
	if g := m.TryAcquire(n, owner); g != nil {
		return g, nil
	}
	m.mu.Lock()
	m.waits = append(m.waits, WaitEvent{Owner: owner, Rows: n, Reason: reason})
	m.mu.Unlock()
	for {
		m.mu.Lock()
		ch := m.ch
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
		if g := m.TryAcquire(n, owner); g != nil {
			return g, nil
		}
	}
}

// NoteSpill 记录一次落盘事件（由算子在 TryAcquire 失败或超上限时调用）。
func (m *MemManager) NoteSpill(owner string, rows int, reason string) {
	m.mu.Lock()
	m.spills = append(m.spills, SpillEvent{Owner: owner, Rows: rows, Reason: reason})
	m.mu.Unlock()
}

// NoteWait 记录一次无法立即获得额度但调用方选择降级继续的事件。
func (m *MemManager) NoteWait(owner string, rows int, reason string) {
	m.mu.Lock()
	m.waits = append(m.waits, WaitEvent{Owner: owner, Rows: rows, Reason: reason})
	m.mu.Unlock()
}

// Stats 返回当前额度池快照。
func (m *MemManager) Stats() MemStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := MemStats{Capacity: m.cap, InUse: m.used, HighWater: m.high}
	s.Waits = append(s.Waits, m.waits...)
	s.Spills = append(s.Spills, m.spills...)
	return s
}

// Capacity 返回总额度（行）。
func (m *MemManager) Capacity() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cap
}
