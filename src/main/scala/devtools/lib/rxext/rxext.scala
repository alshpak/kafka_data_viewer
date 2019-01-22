package devtools.lib.rxext

import io.reactivex.functions.BiFunction
//import io.reactivex.subjects.Subject
//import io.reactivex.{Observable, ObservableSource}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

object RxExtOps {

//    trait ScalaRxCtx
//
//    implicit def seqToObservableOfSeq[T](items: Seq[T]): Observable[Seq[T]] = Observable.just(items)
//
//    implicit def objectToObservableOfObj[T](t: T): Observable[T] = Observable.just(t)
//
//    implicit def objectToSome[T](t: T): Option[T] = Some(t)

    //implicit def objectToSomeOfObservableOfObj[T](t: T): Option[Observable[T]] = objectToObservableOfObj(t)

//    implicit def f2tof2[T1, T2, R](f: (T1, T2) => R): BiFunction[T1, T2, R] = (t1, t2) => f(t1, t2)


//    implicit class ObsForEachOp[T](obs: Observable[T]) {
//        def foreach(f: T => Unit): Unit = obs.subscribe(x => f(x))
//
////        def cacheLatest(default: T): () => T = {
////            var latest = default
////            for (value <- obs) latest = value
////            () => latest
////        }
////
////        def cacheLatest(): () => Option[T] = {
////            var latest: Option[T] = None
////            for (value <- obs) latest = Some(value)
////            () => latest
////        }
////
//        def map[R](f: T => R)(implicit sc:ScalaRxCtx): Observable[R] = obs.map(x => f(x))
//
//        def withLatestFrom[T2, R](t2: Observable[T2])(combiner: (T, T2) => R): Observable[R] =
//            obs.withLatestFrom(t2, (e1: T, e2: T2) => combiner(e1, e2))
//    }

//    implicit class SubjOps[T](subj: Subject[T]) {

//        private val cachedValue = subj.cacheLatest()
//
//        def resend(): Unit = {
//            for (value <- cachedValue()) subj next value
//        }
//    }


}

object ListChangeOps {

    import RxExtOps._

    trait ListChangeOp[T]

    case class SetList[T](items: Seq[T]) extends ListChangeOp[T]

    case class InsertItems[T](index: Integer = 0, items: Seq[T]) extends ListChangeOp[T]

    case class AddItems[T](items: Seq[T]) extends ListChangeOp[T]

    case class RemoveItems[T](index: Integer, amount: Integer = 1) extends ListChangeOp[T]

    case class RemoveItemObjs[T](items: Seq[T]) extends ListChangeOp[T]

    implicit class ObsForListChangeOp[T](obs: Observable[ListChangeOp[T]]) {
        def fromListOps(): Observable[Seq[T]] = {
            val result = ArrayBuffer[T]()
            obs.map {
                case SetList(seq) => result.clear(); result ++= seq; result
                case InsertItems(pos, seq) => result.insertAll(pos, seq); result
                case AddItems(seq) => result ++= seq; result
                case RemoveItems(pos, count) => result.remove(pos, count); result
                case RemoveItemObjs(items) => items.foreach(item => result -= item); result
            }
        }
    }

}

