package com.linkedin.uif.scheduler;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.collect.Lists;

import com.linkedin.uif.metastore.FsStateStore;
import com.linkedin.uif.metastore.StateStore;

/**
 * A utility for migrating task states due to a recent package change.
 *
 * <p>
 *     TODO: delete this class and the {@link TaskState} class after the migration is done.
 * </p>
 */
public class TaskStateMigrationUtil {

    /**
     * Migrate existing task states.
     *
     * @param fsUri File system URI
     * @param srcDir Source task state store directory
     * @param destDir Destination task state store directory
     */
    public static void migrateTaskStates(String fsUri, String srcDir, String destDir) throws IOException {
        StateStore srcStore = new FsStateStore(fsUri, srcDir, TaskState.class);
        StateStore destStore = new FsStateStore(
                fsUri, destDir, com.linkedin.uif.runtime.TaskState.class);

        FileSystem fs = FileSystem.get(URI.create(fsUri), new Configuration());

        FileStatus[] stores = fs.listStatus(new Path(srcDir));
        if (stores == null || stores.length == 0) {
            System.out.println("No task states to migrate");
            return;
        }

        for (FileStatus store : stores) {
            String storeName = store.getPath().getName();

            FileStatus[] tables = fs.listStatus(store.getPath(), new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    // We are only migrating task states
                    return path.getName().endsWith(".tst");
                }
            });
            if (tables == null || tables.length == 0) {
                System.out.println("No task states in store " + storeName);
                continue;
            }

            sortFileStatuses(tables);

            for (FileStatus table : tables) {
                String tableName = table.getPath().getName();
                List<TaskState> srcTaskStates = (List<TaskState>) srcStore.getAll(storeName, tableName);
                destStore.putAll(storeName, tableName, createNewTaskStates(srcTaskStates));
            }

            // After sorting, the first table in the array has the largest modification time
            destStore.createAlias(storeName, tables[0].getPath().getName(), "current" + ".tst");
        }
    }

    /**
     * Migrate existing job states.
     *
     * @param fsUri File system URI
     * @param srcDir Source task state store directory
     * @param destDir Destination task state store directory
     */
    public static void migrateJobStates(String fsUri, String srcDir, String destDir) throws IOException {
        StateStore srcStore = new FsStateStore(fsUri, srcDir, JobState.class);
        StateStore destStore = new FsStateStore(fsUri, destDir, com.linkedin.uif.runtime.JobState.class);

        FileSystem fs = FileSystem.get(URI.create(fsUri), new Configuration());

        FileStatus[] stores = fs.listStatus(new Path(srcDir));
        if (stores == null || stores.length == 0) {
            System.out.println("No job states to copy");
            return;
        }

        for (FileStatus store : stores) {
            String storeName = store.getPath().getName();

            FileStatus[] tables = fs.listStatus(store.getPath(), new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    // We are only copying job states
                    return path.getName().endsWith(".jst");
                }
            });
            if (tables == null || tables.length == 0) {
                System.out.println("No job states in store " + storeName);
                continue;
            }

            sortFileStatuses(tables);

            for (FileStatus table : tables) {
                String tableName = table.getPath().getName();
                List<JobState> jobStates = (List<JobState>) srcStore.getAll(storeName, tableName);
                destStore.putAll(storeName, tableName, createNewJobStates(jobStates));
            }

            // After sorting, the first table in the array has the largest modification time
            destStore.createAlias(storeName, tables[0].getPath().getName(), "current" + ".jst");
        }
    }

    /**
     * Create new task states.
     */
    private static List<com.linkedin.uif.runtime.TaskState> createNewTaskStates(
            List<TaskState> srcTaskStates) {

        List<com.linkedin.uif.runtime.TaskState> destTaskStates = Lists.newArrayList();
        for (TaskState src : srcTaskStates) {
            com.linkedin.uif.runtime.TaskState dest = new com.linkedin.uif.runtime.TaskState(src);
            dest.addAll(src);
            dest.setJobId(src.getJobId());
            dest.setTaskId(src.getTaskId());
            dest.setId(src.getTaskId());
            dest.setStartTime(src.getStartTime());
            dest.setEndTime(src.getEndTime());
            dest.setTaskDuration(src.getTaskDuration());
            dest.setHighWaterMark(src.getPropAsLong("workunit.high.water.mark"));
            dest.setWorkingState(src.getWorkingState());
            destTaskStates.add(dest);
        }

        return destTaskStates;
    }

    /**
     * Create new job states.
     */
    private static List<com.linkedin.uif.runtime.JobState> createNewJobStates(
            List<JobState> srcJobStates) {

        List<com.linkedin.uif.runtime.JobState> destJobStates = Lists.newArrayList();
        for (JobState src : srcJobStates) {
            com.linkedin.uif.runtime.JobState dest = new com.linkedin.uif.runtime.JobState();
            dest.addAll(src);
            dest.setJobName(src.getJobName());
            dest.setJobId(src.getId());
            dest.setId(src.getJobId());
            dest.setState(com.linkedin.uif.runtime.JobState.RunningState.valueOf(src.getState().name()));
            dest.setTasks(src.getTasks());
            dest.setStartTime(src.getStartTime());
            dest.setEndTime(src.getEndTime());
            dest.setDuration(src.getDuration());
            dest.addTaskStates(createNewTaskStates(src.getTaskStates()));
        }

        return destJobStates;
    }

    /**
     * Sort an array of {@link FileStatus} by modification time in descending order.
     */
    private static void sortFileStatuses(FileStatus[] statuses) {
        Arrays.sort(statuses, new Comparator<FileStatus>() {
            @Override
            public int compare(FileStatus fileStatus1, FileStatus fileStatus2) {
                Long modTime1 = fileStatus1.getModificationTime();
                Long modTime2 = fileStatus2.getModificationTime();
                // Sort the file statuses by modification time in descending order
                return -modTime1.compareTo(modTime2);
            }
        });
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: TaskStateMigrationUtil <fs uri> <src store dir> <dest store dir>");
            System.exit(1);
        }

        migrateTaskStates(args[0], args[1], args[2]);
        migrateJobStates(args[0], args[1], args[2]);
    }
}
