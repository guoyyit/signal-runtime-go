package signalruntime

// termCh 在进程被终止时关闭；惰性创建。
func (p *Process) terminationChan() chan struct{} {
	if p.termCh == nil {
		p.termCh = make(chan struct{})
	}
	return p.termCh
}

// waitSignal 阻塞等待一个“已完成交付的捕获信号”，或进程终止。
// 返回 nil 表示收到信号中断；返回 ErrTerminated 表示进程已终止。
func (p *Process) waitSignal() error {
	p.mu.Lock()
	if p.state == procTerminated {
		p.mu.Unlock()
		return ErrTerminated
	}
	ch := p.sigCh
	term := p.terminationChan()
	p.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-term:
		return ErrTerminated
	}
}

// Wait 执行可中断等待。done 为条件完成信号（通常用 close 表达不可逆完成）；
// 不使用 done 时可传 nil，此时仅由信号或终止唤醒。
//
// 确定性规则：每一轮都先判定 done，再消费信号中断。因此当“条件完成”与
// “捕获信号交付”同时发生时，只要 done 已关闭，等待必定成功返回，绝不返回
// ErrInterrupted。等待期间交付一个被捕获信号时：
//   - autoRestart == false：处理器返回后以 ErrInterrupted 返回；
//   - autoRestart == true：回到等待，直到 done 关闭或进程终止。
//
// 被屏蔽而保持待决的信号、被忽略的信号以及默认动作均不打断等待；
// SIGKILL 等导致终止的信号使等待以 ErrTerminated 返回。
func (r *Runtime) Wait(pid int, done <-chan struct{}, autoRestart bool) error {
	p, err := r.mustProcess(pid)
	if err != nil {
		return err
	}

	for {
		// 先判定完成条件。
		if done != nil {
			select {
			case <-done:
				return nil
			default:
			}
		}

		// 在阻塞前确认进程未终止。sigCh 为容量 1 的缓冲通道且在进程锁内
		// 入队，因此“进入等待前已交付的信号”令牌不会丢失。
		p.mu.Lock()
		if p.state == procTerminated {
			p.mu.Unlock()
			return ErrTerminated
		}
		ch := p.sigCh
		term := p.terminationChan()
		p.mu.Unlock()

		if done != nil {
			select {
			case <-done:
				return nil
			case <-ch:
			case <-term:
				return ErrTerminated
			}
		} else {
			select {
			case <-ch:
			case <-term:
				return ErrTerminated
			}
		}

		// 中断处理前再次先判定完成条件，保证条件优先于中断。
		if done != nil {
			select {
			case <-done:
				return nil
			default:
			}
		}
		p.mu.Lock()
		terminated := p.state == procTerminated
		p.mu.Unlock()
		if terminated {
			return ErrTerminated
		}
		if !autoRestart {
			return ErrInterrupted
		}
	}
}

// WaitFn 以轮询谓词 ready 表达完成条件，其余语义与 Wait 相同。
// 注意：谓词条件若可在满足后被撤销，将引入调用方自身的竞争；
// 需要严格确定性时应使用 Wait 并以 close(channel) 表达完成。
func (r *Runtime) WaitFn(pid int, ready func() bool, autoRestart bool) error {
	for {
		if ready() {
			return nil
		}
		// 用一个局部 done 通道无法在外部置位时关闭，因此退化为单次轮询+等待循环。
		p, err := r.mustProcess(pid)
		if err != nil {
			return err
		}
		if err := p.waitSignal(); err != nil {
			return err
		}
		if ready() {
			return nil
		}
		if !autoRestart {
			return ErrInterrupted
		}
	}
}
