package devtools.lib.rxext

import io.reactivex.disposables.Disposable
import io.reactivex.subjects.{BehaviorSubject => JBehaviorSubject, PublishSubject => JPublishSubject, Subject => JSubject}
import io.reactivex.{ObservableOnSubscribe, Scheduler, Observable => JObservable}

import scala.language.implicitConversions

trait Observable[T] {

    import Observable._JObservableConvert

    val wrapped: JObservable[T]

    def map[R](f: T => R): Observable[R] = wrapped.map[R](x => f(x))

    def subscribe(f: T => Unit): Disposable = wrapped.subscribe(x => f(x))

    def foreach(f: T => Unit): Unit = wrapped.subscribe(x => f(x))

    def filter(predicate: T => Boolean): Observable[T] = wrapped.filter(x => predicate(x))

    def withFilter(predicate: T => Boolean): Observable[T] = wrapped.filter(x => predicate(x))

    def withLatestFrom[T2](other: Observable[T2]): Observable[(T, T2)] = wrapped.withLatestFrom[T2, (T, T2)](other.wrapped, (t1: T, t2: T2) => t1 -> t2)

    def subscribeOn(scheduled: Scheduler): Observable[T] = wrapped.subscribeOn(scheduled)

    def observeOn(scheduler: Scheduler): Observable[T] = wrapped.observeOn(scheduler)

    def doOnNext(onNext: T => Unit): Observable[T] = wrapped.doOnNext(t => onNext(t))

    def doOnComplete(onComplete: () => Unit): Observable[T] = wrapped.doOnComplete(() => onComplete())

    def asSubject: Subject[T] = {val subject = Subject.publishSubject[T](); subject <<< this; subject }
}

trait Subject[T] extends Observable[T] {

    override val wrapped: JSubject[T]

    def <<<(obs: Observable[_ <: T]): Unit = obs.wrapped.subscribe(wrapped)

    def <<(t: T): Unit = wrapped onNext t

    def onNext(t: T): Unit = wrapped onNext t

    def onComplete(): Unit = wrapped onComplete()
}

trait PublishSubject[T] extends Subject[T] {

}

trait BehaviorSubject[T] extends Subject[T] {
    override val wrapped: JBehaviorSubject[T]

    def value: T = wrapped.getValue
}


object Observable {

    def apply[T](wrappedInstance: io.reactivex.Observable[T]): Observable[T] = new Observable[T]() {
        override val wrapped: JObservable[T] = wrappedInstance
    }

    def create[T](subscriber: ObservableOnSubscribe[T]): Observable[T] = JObservable.create(subscriber)

    def empty[T](): Observable[T] = JObservable.empty[T]()

    def just[T](items: T*): Observable[T] = JObservable.fromArray[T](items: _*)

    def combineLatest[T1, T2](t1: Observable[T1], t2: Observable[T2]): Observable[(T1, T2)] =
        JObservable.combineLatest[T1, T2, (T1, T2)](t1.wrapped, t2.wrapped, (t1r: T1, t2r: T2) => (t1r, t2r))

    def combineLatest[T1, T2, T3](t1: Observable[T1], t2: Observable[T2], t3: Observable[T3]): Observable[(T1, T2, T3)] =
        JObservable.combineLatest[T1, T2, T3, (T1, T2, T3)](t1.wrapped, t2.wrapped, t3.wrapped, (t1: T1, t2: T2, t3: T3) => (t1, t2, t3))

    //    def combineLatestUsing[T1, T2, R](t1: Observable[T1], t2: Observable[T2])(combine: (T1, T2) => R): Observable[R] =
    //        JObservable.combineLatest(t1, t2, (e1: T1, e2: T2) => combine(e1, e2))
    //
    //
    //    def combineLatestUsing[T1, T2, T3, R](t1: ObservableSource[T1], t2: ObservableSource[T2], t3: ObservableSource[T3])(combine: (T1, T2, T3) => R): Observable[R] =
    //        Observable.combineLatest(t1, t2, t3, new io.reactivex.functions.Function3[T1, T2, T3, R]() {
    //            override def apply(t1: T1, t2: T2, t3: T3): R = combine(t1, t2, t3)
    //        })
    //

    implicit def _JObservableConvert[T](wrapped: JObservable[T]): Observable[T] = Observable(wrapped)
}

object Subject {

    def publishSubject[T](): PublishSubject[T] = new PublishSubject[T] {
        override val wrapped: JSubject[T] = JPublishSubject.create()
    }

    def behaviorSubject[T](): BehaviorSubject[T] = new BehaviorSubject[T] {
        override val wrapped: JBehaviorSubject[T] = JBehaviorSubject.create()
    }

    def behaviorSubject[T](initial: T): BehaviorSubject[T] = new BehaviorSubject[T] {
        override val wrapped: JBehaviorSubject[T] = JBehaviorSubject.createDefault(initial)
    }
}