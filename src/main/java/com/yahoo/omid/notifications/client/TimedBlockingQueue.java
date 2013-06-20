package com.yahoo.omid.notifications.client;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

@SuppressWarnings("unchecked")
public class TimedBlockingQueue<E> implements BlockingQueue<E> {

    private static class TimedElement<E> {
        TimerContext timer;
        E element;

        public TimedElement(TimerContext timer, E element) {
            this.timer = timer;
            this.element = element;
        }
    }

    private BlockingQueue<Object> delegate;
    private Timer timer;

    public TimedBlockingQueue(BlockingQueue<Object> delegate, Timer timer) {
        this.delegate = delegate;
        this.timer = timer;
    }

    @Override
    public E element() {
        return ((TimedElement<E>)delegate.element()).element;
    }

    @Override
    public E peek() {
        TimedElement<E> te = (TimedElement<E>)delegate.peek();
        if (te == null) {
            return null;
        }
        return te.element;
    }

    @Override
    public E poll() {
        TimedElement<E> te = (TimedElement<E>)delegate.poll();
        if (te == null) {
            return null;
        }
        te.timer.stop();
        return te.element;
    }

    @Override
    public E remove() {
        TimedElement<E> te = (TimedElement<E>)delegate.remove();
        te.timer.stop();
        return te.element;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(E e) {
        return delegate.add(new TimedElement<E>(startTimer(), e));
    }

    private TimerContext startTimer() {
        return timer.time();
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(E e) {
        return delegate.offer(new TimedElement<E>(startTimer(), e));
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.offer(new TimedElement<E>(startTimer(), e), timeout, unit);
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        TimedElement<E> te = (TimedElement<E>)delegate.poll(timeout, unit);
        if (te == null) {
            return null;
        }
        te.timer.stop();
        return te.element;
    }

    @Override
    public void put(E e) throws InterruptedException {
        delegate.put(new TimedElement<E>(startTimer(), e));
    }

    @Override
    public int remainingCapacity() {
        return delegate.remainingCapacity();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E take() throws InterruptedException {
        TimedElement<E> te = (TimedElement<E>)delegate.take();
        te.timer.stop();
        return te.element;
    }

}
