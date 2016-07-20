package gobblin.refactor;

public interface JobCatalogListener {
  void onAddJob(JobSpec addedJob);
  void onDeleteJob(JobSpec deletedJob);
  void onUpdateJob(JobSpec originalJob, JobSpec updatedJob);
}
