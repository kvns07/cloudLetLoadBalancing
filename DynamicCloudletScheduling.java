
package simulation;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
//import org.cloudbus.cloudsim.UtilizationModelDynamic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class DynamicCloudletScheduling {

    // DAG dependencies map
    private static Map<Integer, List<Integer>> cloudletDependencies = new HashMap<>();
    private static Map<Integer, List<Integer>> reverseDependencies = new HashMap<>(); // For tracking parent dependencies
    private static Map<Integer, Double> sTime = new HashMap<>();
    public static void main(String[] args) {
        try {
            // Initialize CloudSim
            int numUsers = 1;
            Calendar calendar = Calendar.getInstance();
            CloudSim.init(numUsers, calendar, false);

            // Create Datacenter with Hosts
            Datacenter datacenter = createDatacenter("Datacenter_0");

            // Create Broker
            DatacenterBroker broker = new DatacenterBroker("Broker");

            // Create only 2 VMs
            List<Vm> vmList = createVMs(broker.getId(), 2);

            // Create Cloudlets with different lengths
            List<Cloudlet> cloudletList = createCloudletsWithDifferentLengths(broker.getId(), 8);

            // Configure dependencies for DAG
            configureDAGDependencies();

            // Submit VMs to the broker
            broker.submitVmList(vmList);

            // Submit cloudlets to the broker before assigning them to VMs
            broker.submitCloudletList(cloudletList);

            // Assign cloudlets to VMs using a load balancer
            assignCloudletsToVMs(broker, cloudletList, vmList);

            // Start Simulation
            CloudSim.startSimulation();

            List<Cloudlet> resultList = broker.getCloudletReceivedList();

            // Stop Simulation
            CloudSim.stopSimulation();

            // Print Results
            printCloudletList(resultList,vmList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Datacenter createDatacenter(String name) {
        List<Host> hostList = new ArrayList<>();
        int mips = 1000;
        int ram = 16384; // 16 GB
        long storage = 1000000; // 1 TB
        int bw = 10000; // Bandwidth

        List<Pe> peList = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            peList.add(new Pe(i, new PeProvisionerSimple(mips)));
        }

        Host host = new Host(0, new RamProvisionerSimple(ram), new BwProvisionerSimple(bw), storage, peList, new VmSchedulerTimeShared(peList));
        hostList.add(host);

        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double timeZone = 10.0;
        double costPerSec = 3.0;
        double costPerMem = 0.05;
        double costPerStorage = 0.001;
        double costPerBw = 0.0;

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(arch, os, vmm, hostList, timeZone, costPerSec, costPerMem, costPerStorage, costPerBw);
        Datacenter datacenter = null;
        try {
            datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), new LinkedList<>(), 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }

    // Create only 2 VMs
    private static List<Vm> createVMs(int brokerId, int numVMs) {
        List<Vm> vmList = new ArrayList<>();
//        int mips = 1000;
//        int ram = 2048; // 2 GB RAM
//        long bw = 1000;
//        long size = 10000; // 10 GB
//        String vmm = "Xen";
        String filePath = "C:\\Users\\KVNS\\eclipse-workspace\\mP\\src\\simulation\\vmList.txt"; // Path to the file
        int id=0;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
        		String[] parts = line.split(",\\s*");
                
                // Parse each part to the corresponding type
//                int id = Integer.parseInt(parts[0]);
                int userId = Integer.parseInt(parts[1]);
                double mips = Double.parseDouble(parts[2]);
                int numberOfPes = Integer.parseInt(parts[3]);
                int ram = Integer.parseInt(parts[4]);
                long bw = Long.parseLong(parts[5]);
                long size = Long.parseLong(parts[6]);
                String vmm = parts[7];
            	vmList.add(new Vm(id++, brokerId, mips, numberOfPes, ram, bw, size, vmm, new CloudletSchedulerTimeShared()));
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        for (int i = 0; i < numVMs; i++) {
//            vmList.add(new Vm(i, brokerId, mips, 1, ram, bw, size, vmm, new CloudletSchedulerTimeShared()));
//        }
        return vmList;
    }
    public static class UtilizationModelDynamic implements UtilizationModel {
        private double utilization;

        public UtilizationModelDynamic(double initialUtilization) {
            this.utilization = initialUtilization;
        }

        @Override
        public double getUtilization(double time) {
            // Example: Increase utilization over time by 0.01 per time unit
            return Math.min(1.0, utilization + 0.01 * time);
        }

        public void setUtilization(double utilization) {
            this.utilization = utilization;
        }
    }

    // Create cloudlets with different lengths
    private static List<Cloudlet> createCloudletsWithDifferentLengths(int brokerId, int numCloudlets) {
        List<Cloudlet> cloudletList = new ArrayList<>();
//        long[] lengths = {20000, 30000, 40000, 25000, 35000, 45000, 30000, 40000}; // Different lengths
//        int pesNumber = 1;
//        long fileSize = 300;
//        long outputSize = 300;
//        UtilizationModel utilizationModel = new UtilizationModelFull();
        UtilizationModelDynamic utilizationModelCpu = new UtilizationModelDynamic(0.5); // 50% initial CPU usage
        UtilizationModelDynamic utilizationModelRam = new UtilizationModelDynamic(0.5);
        String filePath = "C:\\Users\\KVNS\\eclipse-workspace\\mP\\src\\simulation\\conf.txt"; // Path to the file
        int id=0;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
        		String[] parts = line.split(",\\s*");
        		long length = Long.parseLong(parts[1]);
                int numberOfPes = Integer.parseInt(parts[2]);
                int fileSize = Integer.parseInt(parts[3]);
//                long outputSize = Long.parseLong(parts[4]);
                Cloudlet cloudlet = new Cloudlet(id++, length, numberOfPes, fileSize, 3000, utilizationModelCpu, utilizationModelRam, new UtilizationModelFull());
	            cloudlet.setUserId(brokerId);
	            cloudletList.add(cloudlet);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        for (int i = 0; i < numCloudlets; i++) {
//            Cloudlet cloudlet = new Cloudlet(i, lengths[i], pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
//            cloudlet.setUserId(brokerId);
//            cloudletList.add(cloudlet);
//        }
        return cloudletList;
    }

    // Configure DAG dependencies for cloudlets
    private static void configureDAGDependencies() {
        // Example: Cloudlet 0 -> 1, Cloudlet 1 -> 2, Cloudlet 2 -> 3
    	String filePath = "C:\\Users\\KVNS\\eclipse-workspace\\mP\\src\\simulation\\dag.txt"; // Path to the file
    	try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
        		String[] parts = line.split(",\\s*");
        		int u=Integer.parseInt(parts[0]);
        		int v=Integer.parseInt(parts[1]);
        		addDependency(u, v);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        addDependency(0, 1);
//        addDependency(1, 2);
//        addDependency(2, 3);
//        addDependency(4, 5);
//        addDependency(5, 6);
//        addDependency(6, 7);
//        addDependency(0, 5);
    }

    // Add a dependency between two cloudlets
    private static void addDependency(int from, int to) {
        cloudletDependencies.computeIfAbsent(from, k -> new ArrayList<>()).add(to);
        reverseDependencies.computeIfAbsent(to, k -> new ArrayList<>()).add(from); // Reverse map for parent tracking
    }

    // Assign cloudlets to VMs considering dependencies using BFS for topological sorting
    private static void assignCloudletsToVMs(DatacenterBroker broker, List<Cloudlet> cloudletList, List<Vm> vmList) {
        Map<Integer, Double> vmFinishTimeMap = new HashMap<>(); // VM ID to its current finish time
        Map<Integer, Double> cloudletFinishTimeMap = new HashMap<>(); // Cloudlet ID to its finish time

        // Initialize VM finish times to 0
        for (Vm vm : vmList) {
            vmFinishTimeMap.put(vm.getId(), 0.0);
        }

        // Queue for BFS topological sorting
        Queue<Integer> queue = new LinkedList<>();
        int[] indegree = new int[cloudletList.size()];

        // Compute in-degrees
        for (int i = 0; i < cloudletList.size(); i++) {
            if (cloudletDependencies.containsKey(i)) {
                for (int dependent : cloudletDependencies.get(i)) {
                    indegree[dependent]++;
                }
            }
        }

        // Add all cloudlets with zero in-degree to the queue
        for (int i = 0; i < cloudletList.size(); i++) {
            if (indegree[i] == 0) {
                queue.add(i);
            }
        }

        // Process the cloudlets in topological order
        while (!queue.isEmpty()) {
            int cloudletId = queue.poll();
            Cloudlet cloudlet = cloudletList.get(cloudletId);
            int selectedVmId = -1;
            double earliestFinishTime = Double.MAX_VALUE;

            // Find the VM with the earliest available finish time
            //totqmemory AVAILABLE
            //MEMERY UTILAIZATON
            //BALANCED MEMORY
            for (Vm vm : vmList) {
                double vmFinishTime = vmFinishTimeMap.get(vm.getId());
                if (vmFinishTime < earliestFinishTime) {
                    earliestFinishTime = vmFinishTime;
                    selectedVmId = vm.getId();
                }
            }

            // Calculate the start time considering parent dependencies
            double startTime = earliestFinishTime;
            if (reverseDependencies.containsKey(cloudletId)) {
                for (int parent : reverseDependencies.get(cloudletId)) {
                    startTime = Math.max(startTime, cloudletFinishTimeMap.getOrDefault(parent, 0.0));
                }
            }

            double executionTime = cloudlet.getCloudletLength() / 1000.0; // Simplified execution time calculation
            double finishTime = startTime + executionTime;

            // Assign cloudlet to the selected VM
            cloudlet.setVmId(selectedVmId);
            broker.bindCloudletToVm(cloudlet.getCloudletId(), selectedVmId);

            // Update the finish time for the selected VM and cloudlet
            vmFinishTimeMap.put(selectedVmId, finishTime);
            cloudletFinishTimeMap.put(cloudletId, finishTime);
            System.out.print(cloudletId);
            System.out.print(" ");
            System.out.println(finishTime+" Start time is "+startTime);
            // Set the start time in the cloudlet
            cloudlet.setExecStartTime(startTime);
            sTime.put(cloudletId, startTime);
            // Process all dependent cloudlets
            if (cloudletDependencies.containsKey(cloudletId)) {
                for (int dependent : cloudletDependencies.get(cloudletId)) {
                    indegree[dependent]--;
                    if (indegree[dependent] == 0) {
                        queue.add(dependent);
                    }
                }
            }
        }
    }

    // Print cloudlet results
    private static void printCloudletList(List<Cloudlet> list,List<Vm> vmList) {
        String indent = "    ";
        DecimalFormat dft = new DecimalFormat("###.##");
        System.out.println("========== OUTPUT ==========");
        System.out.println("Cloudlet ID" + indent + "STATUS" + indent + "Data center ID" + indent + "VM ID" +
                indent + "Execution Time" + indent + "Start Time" + indent + "Finish Time" + indent + "CPU Usage" +indent + "Total memory"+ indent + "Memory Usage");

        for (Cloudlet cloudlet : list) {
            double startTime = sTime.getOrDefault(cloudlet.getCloudletId(),0.0);
            double executionTime = cloudlet.getCloudletLength() / 1000.0; // Simplified execution time calculation
            double finishTime = startTime + executionTime;
            Vm vm = vmList.get(cloudlet.getVmId());
//            System.out.println(cloudlet.getUtilizationOfCpu(finishTime));
            double cpuUsage = vm.getMips() * cloudlet.getUtilizationOfCpu(finishTime); // Calculate CPU usage
            double memoryUsage = vm.getRam() * cloudlet.getUtilizationOfRam(finishTime); // Calculate Memory usage

            System.out.print(indent + cloudlet.getCloudletId() + "  "+indent + indent);
            if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
                System.out.println("SUCCESS" + indent+indent + indent + cloudlet.getResourceId() +indent+ indent + indent + cloudlet.getVmId()+indent + indent + indent + dft.format(executionTime)+indent + indent + indent + dft.format(startTime)+indent + indent + indent + dft.format(finishTime)+ 
                		indent+indent + indent + dft.format(cpuUsage) +
                		indent +indent+ indent + dft.format(vm.getRam())+indent + indent + dft.format(memoryUsage));
            }
        }
    }
}
