package com.linkedin.uif.scheduler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;

/**
 * Unit tests for {@link TaskState}.
 *
 * @author ynli
 */
@Test(groups = {"com.linkedin.uif.scheduler"})
public class TaskStateTest {

    private TaskState taskState;
    private long startTime;

    @BeforeClass
    public void setUp() {
        WorkUnitState workUnitState = new WorkUnitState();
        workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, "Job-1");
        workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, "Task-1");
        this.taskState = new TaskState(workUnitState);
    }

    @Test
    public void testSetAndGet() {
        this.taskState.setId("Task-1");
        this.taskState.setHighWaterMark(2000);
        this.startTime = System.currentTimeMillis();
        this.taskState.setStartTime(this.startTime);
        this.taskState.setEndTime(this.startTime + 1000);
        this.taskState.setTaskDuration(1000);
        this.taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
        this.taskState.setProp("foo", "bar");

        Assert.assertEquals(this.taskState.getJobId(), "Job-1");
        Assert.assertEquals(this.taskState.getTaskId(), "Task-1");
        Assert.assertEquals(this.taskState.getId(), "Task-1");
        Assert.assertEquals(this.taskState.getHighWaterMark(), 2000);
        Assert.assertEquals(this.taskState.getStartTime(), this.startTime);
        Assert.assertEquals(this.taskState.getEndTime(), this.startTime + 1000);
        Assert.assertEquals(this.taskState.getTaskDuration(), 1000);
        Assert.assertEquals(this.taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
        Assert.assertEquals(this.taskState.getProp("foo"), "bar");
    }

    @Test(dependsOnMethods = {"testSetAndGet"})
    public void testSerDe() throws IOException {
        ByteArrayOutputStream baos = null;
        DataOutputStream dos = null;
        ByteArrayInputStream bais = null;
        DataInputStream dis = null;

        try {
            baos = new ByteArrayOutputStream();
            dos = new DataOutputStream(baos);
            this.taskState.write(dos);

            bais = new ByteArrayInputStream(baos.toByteArray());
            dis = new DataInputStream(bais);
            TaskState newTaskState = new TaskState();
            newTaskState.readFields(dis);

            Assert.assertEquals(newTaskState.getJobId(), "Job-1");
            Assert.assertEquals(newTaskState.getTaskId(), "Task-1");
            Assert.assertEquals(this.taskState.getHighWaterMark(), 2000);
            Assert.assertEquals(newTaskState.getStartTime(), this.startTime);
            Assert.assertEquals(newTaskState.getEndTime(), this.startTime + 1000);
            Assert.assertEquals(newTaskState.getTaskDuration(), 1000);
            Assert.assertEquals(newTaskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
            Assert.assertEquals(newTaskState.getProp("foo"), "bar");
        } finally {
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException ioe) {
                    // Ignored
                }
            }

            if (dos != null) {
                try {
                    dos.close();
                } catch (IOException ioe) {
                    // Ignored
                }
            }

            if (bais != null) {
                try {
                    bais.close();
                } catch (IOException ioe) {
                    // Ignored
                }
            }

            dis.close();
        }
    }
}
