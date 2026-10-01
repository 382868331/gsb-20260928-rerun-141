package relengine

// BuildNestedLoopJoin 构造遵守同一批大小和行内存预算的块嵌套循环连接。
func BuildNestedLoopJoin(l, r Executor, on Expr, jt JoinType, ln, rn int, opts ExecOptions) Executor {
	return newBoundedJoin(l, r, on, jt, ln, rn, nil, nil, opts)
}
