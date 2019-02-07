package devtools.lib.rxext

import java.lang

import io.reactivex.disposables.Disposable
import io.reactivex.subjects.{BehaviorSubject => JBehaviorSubject, PublishSubject => JPublishSubject, Subject => JSubject}
import io.reactivex.{ObservableOnSubscribe, ObservableSource, Scheduler, functions, Observable => JObservable}

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import Observable._

trait Observable[T] {

    import Observable._JObservableConvert

    val wrapped: JObservable[T]

    def map[R](f: T => R): Observable[R] = wrapped.map[R](x => f(x))

    def flatMap[R](f: T => Observable[R]): Observable[R] = {
        val fun: functions.Function[T, JObservable[R]] = (t: T) => f(t).wrapped
        wrapped.flatMap(fun)
    }

    def subscribe(f: T => Unit): Disposable = wrapped.subscribe(x => f(x))

    def foreach(f: T => Unit): Unit = wrapped.subscribe(x => f(x))

    def filter(predicate: T => Boolean): Observable[T] = wrapped.filter(x => predicate(x))

    def withFilter(predicate: T => Boolean): Observable[T] = wrapped.filter(x => predicate(x))

    def withLatestFrom[T2](other: Observable[T2]): Observable[(T, T2)] = wrapped.withLatestFrom[T2, (T, T2)](other.wrapped, (t1: T, t2: T2) => t1 -> t2)

    def withLatestFrom[T2, T3](source2: Observable[T2], source3: Observable[T3]): Observable[(T, T2, T3)] =
        wrapped.withLatestFrom[T2, T3, (T, T2, T3)](source2.wrapped, source3.wrapped, (t1: T, t2: T2, t3: T3) => (t1, t2, t3))

    def withLatestFrom[T2, T3, T4](source2: Observable[T2], source3: Observable[T3], source4: Observable[T4]): Observable[(T, T2, T3, T4)] =
        wrapped.withLatestFrom[T2, T3, T4, (T, T2, T3, T4)](source2.wrapped, source3.wrapped, source4.wrapped, (t1: T, t2: T2, t3: T3, t4: T4) => (t1, t2, t3, t4))

    def subscribeOn(scheduled: Scheduler): Observable[T] = wrapped.subscribeOn(scheduled)

    def observeOn(scheduler: Scheduler): Observable[T] = wrapped.observeOn(scheduler)

    def doOnNext(onNext: T => Unit): Observable[T] = wrapped.doOnNext(t => onNext(t))

    def doOnComplete(onComplete: () => Unit): Observable[T] = wrapped.doOnComplete(() => onComplete())

    def asPublishSubject: Subject[T] = {val subject = Subject.publishSubject[T](); subject <<< this; subject }

    def withCachedLatest(): Observable[T] = Observable[T] { val c = wrapped.replay(1); c.connect(); c }
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

    def merge[T](sources: Seq[Observable[_ <: T]]): Observable[T] = {
        val iterable: lang.Iterable[ObservableSource[T]] = sources.map(_.wrapped).asJavaCollection.asInstanceOf[lang.Iterable[ObservableSource[T]]]
        val value: JObservable[T] = JObservable.merge(iterable)
        Observable(value)
    }

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

object ObservableSeqExt {

    implicit class ObservableSeqOps[T](obs: Observable[_ <: Seq[T]]) {
        def mapSeq[R](f: T => R): Observable[Seq[R]] = obs.map(_.map(f))

        def flatMapSeq[R](f: T => Seq[R]): Observable[Seq[R]] = obs.map(_.flatMap(f))
    }

}

