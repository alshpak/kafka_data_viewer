package devtools.kafka_data_viewer.kafkaconn

import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, ConsumerGroupService}
import org.apache.kafka.common.Node

object KafkaGroupsInfo {

    case class PartitionAssignmentState(group: String, coordinator: Option[Node], topic: Option[String],
                                        partition: Option[Int], offset: Option[Long], lag: Option[Long],
                                        consumerId: Option[String], host: Option[String],
                                        clientId: Option[String], logEndOffset: Option[Long])


    // --bootstrap-server kafka-01-int-01.privalia.aws:9092
    // --describe --group oms-group-1 --bootstrap-server kafka-01-int-01.privalia.aws:9092

    //  --list --zookeeper zookeeper-int-01-node01.privalia.aws

    def getKafkaGroups(kafkaHost: String): Seq[String] = {
        val opts = new ConsumerGroupCommandOptions(Array("--bootstrap-server", kafkaHost))
        val groupService = new ConsumerGroupService(opts)
        try {
            groupService.listGroups()
        } finally {
            groupService.close()
        }
    }

    def getZooGroups(zooHost: String): Seq[String] = {
//        val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zooHost))
//        val groupService = new ZkConsumerGroupService(opts)
//        try {
//            groupService.listGroups()
//        } finally {
//            groupService.close()
//        }
        Seq()
    }

    def describeKafkaGroup(kafkaHost: String, group: String): Seq[PartitionAssignmentState] = {
        val opts = new ConsumerGroupCommandOptions(Array("--bootstrap-server", kafkaHost, "--group", group))
        val groupService = new ConsumerGroupService(opts)
        try {
            val res = groupService.collectGroupOffsets()
            val states = res._2.getOrElse(Seq())
            states.map(state => PartitionAssignmentState(
                group = state.group, coordinator = state.coordinator, topic = state.topic, partition = state.partition,
                offset = state.offset, lag = state.lag, consumerId = state.consumerId, host = state.host, clientId = state.clientId,
                logEndOffset = state.logEndOffset))
        } finally {
            groupService.close()
        }
    }

    def describeZooGroup(zooHost: String, group: String): Seq[PartitionAssignmentState] = {
//        val opts = new ConsumerGroupCommandOptions(Array("--zookeeper", zooHost, "--group", group))
//        val groupService = new ZkConsumerGroupService(opts)
//        try {
//            val res = groupService.collectGroupOffsets()
//            val states = res._2.getOrElse(Seq())
//            states.map(state => PartitionAssignmentState(
//                group = state.group, coordinator = state.coordinator, topic = state.topic, partition = state.partition,
//                offset = state.offset, lag = state.lag, consumerId = state.consumerId, host = state.host, clientId = state.clientId,
//                logEndOffset = state.logEndOffset))
//        } finally {
//            groupService.close()
//        }
        Seq()
    }

}
