package signalruntime

// Action 表示信号处置方式。
type Action int

const (
	// ActDefault 执行默认动作。
	ActDefault Action = iota
	// ActIgnore 忽略信号。
	ActIgnore
	// ActCatch 交由自定义处理器处理。
	ActCatch
)

// HandlerFunc 是自定义信号处理器。value 为实时信号投递时携带的随队值，
// 标准信号恒为 0。
type HandlerFunc func(sig Signal, value int)

// SigAction 描述一个信号的处置配置。
type SigAction struct {
	// Action 为处置方式。
	Action Action
	// Handler 仅在 Action == ActCatch 时使用。
	Handler HandlerFunc
	// Name 为自定义处理器的稳定名称，用于崩溃恢复后重新绑定处理器。
	// 持久化时记录 Name；恢复时通过 Restore 提供的解析函数找回 Handler。
	// Name 为空的捕获处置在恢复后退化为默认动作。
	Name string
	// Mask 为处理器执行期间附加屏蔽的信号集合（处理器屏蔽字）。
	Mask SigSet
}
