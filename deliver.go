package signalruntime

// receive 处理一个到达本进程的信号实例：校验状态、记入待决集合，
// 然后尽可能地完成交付。整个过程对本进程串行化。
func (p *Process) receive(rt *Runtime, sig Signal, value int, seq uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == procTerminated {
		return ErrTerminated
	}
	if !p.enqueue(sig, value, seq) {
		return ErrQueueFull
	}
	p.deliver(rt)
	return nil
}

// deliver 尝试交付进程上所有当前可交付的待决信号（骨架）。
// 调用方需自行保证序列化。
func (p *Process) deliver(rt *Runtime) {
	for {
		_, ok, terminated := p.pickRound(rt)
		if terminated {
			return
		}
		if !ok {
			return
		}
	}
}

// pickRound 执行一“轮”交付：从 cursor 起在 1..SIGRTMAX 上环形扫描一圈，
// 每轮内每个信号编号至多交付一个实例；真正完成一次交付后该轮结束并返回，
// 下一轮从被交付编号的下一个编号开始。扫描中遇到被屏蔽、停止而暂缓或无待决
// 的编号不推进 cursor，从而这些位解除阻塞后仍能从其位置继续；而持续可交付的
// 低编号信号在交付一轮后必须绕完一整圈才能再次被选中，因此无法饿死高编号。
// 返回的第三值表示进程是否已终止。
// 进入时调用方持有 mu；自定义处理器在锁外执行。
func (p *Process) pickRound(rt *Runtime) (q queuedSignal, found bool, terminated bool) {
	start := p.cursor
	for n := 0; n < signalCount-1; n++ {
		sig := Signal(((int(start) - 1 + n) % (signalCount - 1)) + 1)
		if !p.hasPending(sig) {
			continue
		}
		// 停止状态下除 SIGKILL/SIGCONT 外全部保持待决。
		if p.state == procStopped && sig != SIGKILL && sig != SIGCONT {
			continue
		}
		// SIGKILL/SIGSTOP 不能被阻塞；其余信号尊重当前叠加屏蔽字。
		if !isUncatchable(sig) && p.masked(sig) {
			continue
		}
		instance, ok := p.removePending(sig)
		if !ok {
			continue
		}
		// 交付前推进轮转载入点，保证本信号下一轮排在所有其他编号之后。
		p.cursor = sig + 1
		if int(p.cursor) >= signalCount {
			p.cursor = 1
		}
		// dispatch 可能在锁外执行处理器并在返回后继续交付（嵌套循环）。
		// 本轮到此结束：嵌套返回后由外层循环从新 cursor 开启下一轮。
		p.dispatch(rt, instance)
		return queuedSignal{}, true, p.state == procTerminated
	}
	return queuedSignal{}, false, false
}

// dispatch 对一个已离开待决队列的实例执行处置动作。
// 调用方须持有 mu；自定义处理器在锁外执行。
func (p *Process) dispatch(rt *Runtime, q queuedSignal) {
	sig := q.sig

	// 不可捕获信号永远执行默认动作，任何处置配置都被忽略。
	if isUncatchable(sig) {
		p.applyDefault(sig)
		return
	}

	// 停止状态下被交付的只可能是 SIGCONT。
	if p.state == procStopped {
		if sig == SIGCONT {
			p.doContinue()
			// SIGCONT 自身若被捕获，继续动作完成后仍交付给处理器。
			if p.actionFor(sig).Action == ActCatch {
				p.runHandler(rt, q)
			}
		}
		return
	}

	act := p.actionFor(sig)
	switch act.Action {
	case ActIgnore:
		// 丢弃，不产生任何交付，也不打断等待。
		return
	case ActCatch:
		p.runHandler(rt, q)
		return
	default:
		p.applyDefault(sig)
	}
}

// applyDefault 执行信号的默认动作。调用方须持有 mu。
func (p *Process) applyDefault(sig Signal) {
	switch defaultActionFor(sig) {
	case defStop:
		if p.state == procRunning {
			p.state = procStopped
		}
	case defContinue:
		p.doContinue()
	default:
		// defTerminate / defCore：进程终止，未交付的待决信号随之失效。
		p.state = procTerminated
		p.rtQueue = nil
		p.stdPending = SigSet{}
		if p.termCh != nil {
			close(p.termCh)
			p.termCh = nil
		}
	}
}

// doContinue 执行 POSIX 继续语义：恢复运行并丢弃待决的作业控制停止信号；
// 其余待决信号（含全部实时信号）保持原样，随后按轮转规则交付。
// 调用方须持有 mu。
func (p *Process) doContinue() {
	if p.state != procStopped {
		return
	}
	p.state = procRunning
	for s := Signal(1); s < SIGRTMIN; s++ {
		if isStopSignal(s) && p.stdPending.Has(s) {
			p.stdPending = p.stdPending.Remove(s)
		}
	}
}

// runHandler 在锁外执行自定义处理器。处理器执行期间的有效屏蔽字为
// 当前屏蔽字 ∪ 处理器屏蔽字 ∪ {当前信号}，因此同信号处理器不可重入；
// 处理器返回后恢复屏蔽字并打断该进程上的阻塞等待。
// 进入时调用方持有 mu；处理器执行期间释放锁。
func (p *Process) runHandler(rt *Runtime, q queuedSignal) {
	sig := q.sig
	act := p.actionFor(sig)

	savedMask := p.mask
	handlerMask := act.Mask.Union(NewSigSet(sig))
	p.mask = p.mask.Union(handlerMask)
	p.runningHandlers = p.runningHandlers.Add(sig)
	p.inflight = append(p.inflight, q)
	handler := act.Handler

	p.mu.Unlock()
	func() {
		defer func() { _ = recover() }()
		handler(sig, q.value)
	}()
	p.mu.Lock()

	p.inflight = removeInflight(p.inflight, q)
	p.runningHandlers = p.runningHandlers.Remove(sig)
	p.mask = savedMask

	// 处理器执行期间该进程可能已被终止。
	if p.state != procTerminated {
		p.signalInterrupt()
	}
}

// removeInflight 从在途列表中移除一个已完成的实例。
func removeInflight(list []queuedSignal, q queuedSignal) []queuedSignal {
	for i, x := range list {
		if x.seq == q.seq && x.sig == q.sig {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}
