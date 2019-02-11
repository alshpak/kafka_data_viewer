import devtools.lib.rxext.{Observable, Subject}

object TestObservableOps {

    def main(args: Array[String]): Unit = {

        val source =
            //Observable.just("one", "two", "three")
            behaviourSource()
        //simpleColdSource()

        val sourceMapped = source.map(x => x + " Mapped")
                    .doOnNext(x => println("Mapping is done"))

        val tgt = Observable {
            val c = sourceMapped.wrapped.retry()
//            c.connect()
            c
        }

        Thread.sleep(1500)

//        tgt.foreach(x => println("Consumer 1: " + x))
//        tgt.foreach(x => println("Consumer 2: " + x))
//        tgt.foreach(x => println("Consumer 3: " + x))


        Thread.sleep(5000L)
    }

    def simpleColdSource(): Observable[String] = {
        val source = Observable.create[String](subscriber => {
            subscriber.onNext("One")
            subscriber.onNext("Two")
        })
        source.wrapped.replay(1)
    }

    def behaviourSource(): Observable[String] = {
        val source = Subject.behaviorSubject("One")
        new Thread(() => {
            Thread.sleep(1000L)
            source << "Two"
            Thread.sleep(1000L)
            source << "Three"
        }).start()
        source.wrapped
    }

    def publishSubject(): Observable[String] = {
        val source = Subject.publishSubject[String]()
        new Thread(() => {
            Thread.sleep(200L)
            source << "Two"
            Thread.sleep(200L)
            source << "Three"
        }).start()
        source

    }

}
