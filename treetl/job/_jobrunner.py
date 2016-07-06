
import logging
from treetl.tools.joblogging import JobRunnerLogger

from treetl.job._job import Job
from treetl.tools import build_enum
from treetl.tools.polytree import PolyTree, TreeNode


job_runner_logger = JobRunnerLogger(logging.getLogger(__name__))


JOB_STATUS = build_enum('QUEUE', 'RUNNING', 'DONE', 'FAILED')


class JobException(Exception):
    def __init__(self, job=None, *args, **kwargs):
        super(JobException, self).__init__(*args, **kwargs)
        self.job = job


class ParentJobException(JobException):
    def __init__(self, job=None, parent_job=None, *args, **kwargs):
        super(ParentJobException, self).__init__(job, *args, **kwargs)
        self.parent_job = parent_job


class JobNode(TreeNode):
    def __init__(self, job):
        assert isinstance(job, Job)
        super(JobNode, self).__init__(job.__class__.__name__, job)
        self.status = JOB_STATUS.QUEUE
        self.error = None


class JobRunner(object):
    def __init__(self, jobs=None):
        # maintain order of explicitly submitted
        # so that they can easily be retrieved
        self._submitted_job_ids = [ ]

        self.__ptree = PolyTree()
        if jobs:
            self.add_jobs(jobs)

        self.status = JOB_STATUS.QUEUE

    def add_job(self, job):
        # create job node
        job_node = JobNode(job)
        if self.__ptree.node_exists(job_node):
            # explicitly added jobs >> implicit
            # result:
            #   if job added twice, second is kept.
            #   if job was added by parent inference, it's overwritten
            self.__ptree.get_node(job_node.id).data = job

        def get_or_create(parent):
            r = self.__ptree.get_node(parent.__name__)
            if r is not None:
                return r
            else:
                return JobNode(parent())

        # get parents
        parents = [
            get_or_create(parent)
            for parent in job.ETL_SIGNATURE.values()
        ] if hasattr(job, 'ETL_SIGNATURE') else []

        job_runner_logger.add_jobs(job, parents)

        # add to job poly tree
        self.__ptree.add_node(job_node, parents)
        if job_node.id not in self._submitted_job_ids:
            self._submitted_job_ids.append(job_node.id)

        return self

    def add_jobs(self, jobs):
        [ self.add_job(j) for j in jobs ]
        return self

    def __get_job_kwargs(self, job):
        if hasattr(job, 'ETL_SIGNATURE'):
            return {
                param: self.__ptree.get_node(type_source.__name__).data.transformed_data
                for param, type_source in job.ETL_SIGNATURE.items()
            }
        else:
            return {}

    def parents(self, job):
        return self.__ptree.parents(JobNode(job))

    # runs a job and caches if needed
    def __run_single_job(self, job_node):
        job_runner_logger.start_job(job_node.data)

        job_node.status = JOB_STATUS.RUNNING
        try:
            # stage/run job
            job_runner_logger.log_job_method(job_node.data, 'extract')
            job_node.data.extract()

            transform_params = self.__get_job_kwargs(job_node.data)
            job_runner_logger.log_job_method(job_node.data, 'transform', transform_params)
            job_node.data.transform(**transform_params)

            # if there are queued up children jobs, cache results
            rem_children_job_ct = len(self.children_in_queue(job_node.data))
            if rem_children_job_ct > 0:
                job_runner_logger.log_job_method(job_node.data, 'cache', other_info={'children': rem_children_job_ct})
                job_node.data.cache()

            # load results
            job_runner_logger.log_job_method(job_node.data, 'load')
            job_node.data.load()

            # mark job as done and move on
            job_node.status = JOB_STATUS.DONE
            job_runner_logger.completed_job(job_node.data)
        except Exception as e:
            job_runner_logger.job_error(job_node.data)
            job_node.error = JobException(job_node.data, e)
            job_node.status = JOB_STATUS.FAILED

    # runs a job and all its parents
    def __run_job_line(self, job_node):
        # run parent jobs
        for parent in self.parents(job_node.data):
            # no need to walk the whole chain if immediate parent is already done
            check_status = self.__run_job_line(parent) if parent.status == JOB_STATUS.QUEUE else parent.status
            if check_status == JOB_STATUS.FAILED:
                job_runner_logger.skip_job(job_node.data, parent.data)
                job_node.status = JOB_STATUS.FAILED
                job_node.error = ParentJobException(job=job_node.data, parent_job=parent.data)


        # run current job
        if job_node.status == JOB_STATUS.QUEUE:
            self.__run_single_job(job_node)

        # uncache parents that are no longer needed
        for parent in self.__ptree.parents(job_node):
            if len(self.children_in_queue(parent.data)) == 0:
                job_runner_logger.log_job_method(parent.data, 'uncache')
                parent.data.uncache()

        return job_node.status

    def run(self, start_from=None):

        if start_from is not None:
            raise NotImplementedError()

        self.status = JOB_STATUS.RUNNING
        job_runner_logger.log_status(self.status)

        for jn in self.__ptree.end_nodes():
            self.__run_job_line(jn)

        self.status = JOB_STATUS.FAILED if len(self.failed_jobs()) else JOB_STATUS.DONE
        job_runner_logger.log_status(self.status)

        return self

    def children_in_queue(self, job):
        return [
            child_job_node.data
            for child_job_node in self.__ptree.children(JobNode(job))
            if child_job_node.status == JOB_STATUS.QUEUE
        ]

    def job_results(self, job=None, submitted_only=False):
        if job:
            return self.__ptree.get_node(JobNode(job).id)
        elif submitted_only:
            return [
                self.__ptree.get_node(id)
                for id in self._submitted_job_ids
            ]
        else:
            return self.__ptree.nodes()

    def failed_jobs(self):
        return [ node.data for node in self.job_results() if node.status == JOB_STATUS.FAILED ]

    def failed_job_roots(self):
        return [
            node.data
            for node in self.__ptree.nodes()
            if isinstance(node.error, JobException) and not isinstance(node.error, ParentJobException)
        ]

    def failed_job_root_paths(self):
        return {
            fail_root: self.all_paths(fail_root)
            for fail_root in self.failed_job_roots()
        }

    def all_paths(self, job):
        return [
            [ path_item.data for path_item in path ]
            for path in self.__ptree.all_paths(JobNode(job))
        ]

    def submitted_jobs(self):
        return [ j.data for j in self.job_results(submitted_only=True) ]

    def jobs(self):
        return [ node.data for node in self.__ptree.nodes() ]

    def reset_jobs(self):
        for job_node in self.__ptree.nodes():
            job_node.status = JOB_STATUS.QUEUE

    def clear_jobs(self):
        self._submitted_job_ids = []
        return self.__ptree.clear_nodes()
