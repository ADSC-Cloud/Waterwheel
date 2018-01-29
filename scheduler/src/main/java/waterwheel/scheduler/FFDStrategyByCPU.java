package waterwheel.scheduler;

import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.resource.*;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by 44931 on 2017/12/30.
 */
public class FFDStrategyByCPU implements IStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(FFDStrategyByCPU.class);
    private Cluster _cluster;
    private Topologies _topologies;
    private Map<String, List<String>> _clusterInfo;
    private RAS_Nodes _nodes;

    @Override
    public void prepare(SchedulingState schedulingState) {
        _cluster = schedulingState.cluster;
        _topologies = schedulingState.topologies;
        _nodes = schedulingState.nodes;
        _clusterInfo = schedulingState.cluster.getNetworkTopography();
    }

    @Override
    public SchedulingResult schedule(TopologyDetails td) {
        LOG.info("FFD strategy start schedule!");
        if (_nodes.getNodes().size() <= 0) {
            LOG.warn("No available nodes to schedule tasks on!");
            return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, "No available nodes to schedule tasks on!");
        }

        Collection<ExecutorDetails> unassignedExecutors = new HashSet<ExecutorDetails>(_cluster.getUnassignedExecutors(td));
        Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap = new HashMap<>();
        Collection<ExecutorDetails> scheduledTasks = new ArrayList<>();
        List<Component> spouts = this.getSpouts(td);

        if (spouts.size() == 0) {
            LOG.error("Cannot find a Spout!");
            return SchedulingResult.failure(SchedulingStatus.FAIL_INVALID_TOPOLOGY, "Cannot find a Spout!");
        }

        //order executors and Nodes to be scheduled
        List<ExecutorDetails> orderedExecutors = orderExecutorsByCPU(td, unassignedExecutors);
        Collection<ExecutorDetails> executorsNotScheduled = new HashSet<>(unassignedExecutors);
        //List<Map.Entry<String, Double>> orderedNodes = orderNodeByCPU(_cluster, _topologies);
        scheduleExecutor(orderedExecutors, td, schedulerAssignmentMap, scheduledTasks);

        SchedulingResult result;
        executorsNotScheduled.removeAll(scheduledTasks);
        if (executorsNotScheduled.size() > 0) {
            LOG.error("Not all executors successfully scheduled: {}",
                    executorsNotScheduled);
            schedulerAssignmentMap = null;
            result = SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES,
                    (td.getExecutors().size() - unassignedExecutors.size()) + "/" + td.getExecutors().size() + " executors scheduled");
        } else {
            LOG.debug("All resources successfully scheduled!");
            result = SchedulingResult.successWithMsg(schedulerAssignmentMap, "Fully Scheduled by waterwheel.FFDStrategyByCPU");
        }
        if (schedulerAssignmentMap == null) {
            LOG.error("Topology {} not successfully scheduled!", td.getId());
        }
        return result;
    }

    private void scheduleExecutor(List<ExecutorDetails> orderedExecutors, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap, Collection<ExecutorDetails> scheduledTasks) {
        for (ExecutorDetails executor : orderedExecutors) {
            double cpuReqTask = td.getTotalCpuReqTask(executor);
            LOG.info(executor.toString() + " need cpu: " + cpuReqTask);
            RAS_Node node = findNodeWithMaxAvilableCPU();
            if (node != null && (node.getAvailableCpuResources() >= cpuReqTask)) {
                Collection<WorkerSlot> workerSlots = node.getFreeSlots();
                WorkerSlot targetSlot = getBestWorker(workerSlots, td, schedulerAssignmentMap);
                if (!schedulerAssignmentMap.containsKey(targetSlot)) {
                    schedulerAssignmentMap.put(targetSlot, new LinkedList<ExecutorDetails>());
                }
                schedulerAssignmentMap.get(targetSlot).add(executor);
                node.consumeResourcesforTask(executor, td);
                scheduledTasks.add(executor);
            } else {
                LOG.error("not Enough Resources to schedule Task {}, require {}, resource availability {}", executor,
                        cpuReqTask,
                        node != null ? node.getAvailableCpuResources() : -1);
                break;
            }
        }
    }

    private RAS_Node findNodeWithMaxAvilableCPU() {
        RAS_Node result = null;
        double maxAvilableCPU = 0.0;
        for (RAS_Node node : _nodes.getNodes()) {
            if (node.getAvailableCpuResources() > maxAvilableCPU) {
                result = node;
                maxAvilableCPU = node.getAvailableCpuResources();
            }
        }
        if (result == null)
            LOG.info("No node with available cpu is found.");
        if (result != null)
            LOG.info("best select is: " + result.getHostname() + ", it have cpu: " + maxAvilableCPU);
        return result;
    }

    private WorkerSlot getBestWorker(Collection<WorkerSlot> workerSlots, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap) {
        WorkerSlot bestWorker = null;
        Double min = Double.MAX_VALUE;
        for (WorkerSlot ws : workerSlots) {
            Double totalCPUUse = 0.0;
            Collection<ExecutorDetails> execs = schedulerAssignmentMap.get(ws);
            if (execs != null) {
                for (ExecutorDetails exec : execs) {
                    totalCPUUse += td.getTotalCpuReqTask(exec);
                }
            }
            if (totalCPUUse < min) {
                min = totalCPUUse;
                bestWorker = ws;
            }
        }
        return bestWorker;
    }


//    private List<Map.Entry<String, Double>> orderNodeByCPU(Cluster cluster, Topologies topologies) {
//        Map<String, Double> supervisorsCPUResource = new HashMap<String, Double>();
//        Map<String, RAS_Node> nodes = RAS_Nodes.getAllNodesFrom(cluster, topologies);
//        for (Map.Entry<String, RAS_Node> entry : nodes.entrySet()) {
//            RAS_Node node = entry.getValue();
//            Double avilableCPU = node.getAvailableCpuResources();
//            supervisorsCPUResource.put(entry.getKey(), avilableCPU);
//        }
//        List<Map.Entry<String, Double>> result = new ArrayList<Map.Entry<String, Double>>(supervisorsCPUResource.entrySet());
//        Collections.sort(result, new AvailableCPUComparator());
//        return result;
//    }

    private List<ExecutorDetails> orderExecutorsByCPU(TopologyDetails td, Collection<ExecutorDetails> unassignedExecutors) {
        HashMap map = new HashMap<ExecutorDetails, Double>();
        for (ExecutorDetails unassignedExecutor : unassignedExecutors) {
            double cpu = td.getTotalCpuReqTask(unassignedExecutor);
            map.put(unassignedExecutor, cpu);
        }
        List<Map.Entry<ExecutorDetails, Double>> list = new ArrayList<Map.Entry<ExecutorDetails, Double>>(map.entrySet());
        Collections.sort(list, new UsageOfCPUComparator().reversed());
        List<ExecutorDetails> result = new LinkedList<>();
        for (Map.Entry<ExecutorDetails, Double> entry : list) {
            result.add(entry.getKey());
        }
        return result;
    }

    /**
     * Get a list of all the spouts in the topology
     *
     * @param td topology to get spouts from
     * @return a list of spouts
     */
    private List<Component> getSpouts(TopologyDetails td) {
        List<Component> spouts = new ArrayList<>();

        for (Component c : td.getComponents().values()) {
            if (c.type == Component.ComponentType.SPOUT) {
                spouts.add(c);
            }
        }
        return spouts;
    }

    private class UsageOfCPUComparator implements Comparator<Map.Entry<ExecutorDetails, Double>> {

        @Override
        public int compare(Map.Entry<ExecutorDetails, Double> o1, Map.Entry<ExecutorDetails, Double> o2) {
            return Double.compare(o1.getValue(), o2.getValue());
//            return (int) (o2.getValue() - o1.getValue());
        }
    }

//    private class AvailableCPUComparator  implements Comparator<Map.Entry<String, Double>> {
//
//        @Override
//        public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
//            return (int) (o2.getValue() - o1.getValue());
//        }
//    }

    public static void main(String[] args) {

    }
}
