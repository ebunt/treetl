
from treetl.tools import build_enum
from treetl.polytree import PolyTree, TreeNode


JOB_STATUS = build_enum('QUEUE', 'RUNNING', 'DONE', 'FAILED')


class Job(object):

    # store the proper param names for transformed
    # data from parent jobs. populated by decorator
    ETL_SIGNATURE = { }

    # add this decorator to populate ETL_SIGNATURE (in a nice looking way)
    @staticmethod
    def dependency(**kwargs):
        def class_wrap(cls):
            cls.ETL_SIGNATURE = {
                param_name: job_type
                for param_name, job_type in kwargs.items()
            } if kwargs else { }
            return cls
        return class_wrap

    @staticmethod
    def create_as_job(job_name, extract=None, transform=None, load=None, cache=None, uncache=None, **kwargs):
        # add support for dynamic job creation
        raise NotImplementedError()

    def __init__(self):
        self.transformed_data = None

    def extract(self, **kwargs):
        return self

    def transform(self, **kwargs):
        return self

    def load(self, **kwargs):
        return self

    def cache(self, **kwargs):
        return self

    def uncache(self, **kwargs):
        return self


class ParentJobException(Exception):
     def __init__(self, parent_job_node, **kwargs):
         super(ParentJobException, self).__init__(**kwargs)
         self.parent_job_node = parent_job_node


class JobNode(TreeNode):
    def __init__(self, job):
        assert isinstance(job, Job)
        super(JobNode, self).__init__(job.__class__.__name__, job)
        self.status = JOB_STATUS.QUEUE
        self.error = None


class JobRunner(object):
    def __init__(self, jobs=None):
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

        # get parents
        parents = [
            JobNode(parent())
            for parent in job.ETL_SIGNATURE.values()
        ] if hasattr(job, 'ETL_SIGNATURE') else []

        # add to job poly tree
        self.__ptree.add_node(job_node, parents)

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

    # runs a job and caches if needed
    def __run_single_job(self, job_node):
        job_node.status = JOB_STATUS.RUNNING
        try:
            job_node.data.extract()

            # stage/run job
            job_node.data.transform(**self.__get_job_kwargs(job_node.data))

            # if there are queued up children jobs, cache results
            if len(self.children_in_queue(job_node.data)) > 0:
                job_node.data.cache()

            # load results
            job_node.data.load()
            job_node.status = JOB_STATUS.DONE
        except Exception as e:
            job_node.error = e
            job_node.status = JOB_STATUS.FAILED

    # runs a job and all its parents
    def __run_job_line(self, job_node):

        # run parent jobs
        for parent in self.__ptree.parents(job_node):
            # no need to walk the whole chain if immediate parent is already done
            check_status = self.__run_job_line(parent) if parent.status == JOB_STATUS.QUEUE else parent.status
            if check_status == JOB_STATUS.FAILED:
                job_node.status = JOB_STATUS.FAILED
                job_node.error = ParentJobException(parent)


        # run current job
        if job_node.status == JOB_STATUS.QUEUE:
            self.__run_single_job(job_node)

        # uncache parents that are no longer needed
        for parent in self.__ptree.parents(job_node):
            if len(self.children_in_queue(parent.data)) == 0:
                parent.data.uncache()

        return job_node.status

    def run_all_jobs(self):
        self.status = JOB_STATUS.RUNNING

        for jn in self.__ptree.end_nodes():
            self.__run_job_line(jn)

        self.status = JOB_STATUS.FAILED if len(self.failed_jobs()) else JOB_STATUS.DONE

        return self

    def children_in_queue(self, job):
        return [
            child_job_node.data
            for child_job_node in self.__ptree.children(JobNode(job))
            if child_job_node.status == JOB_STATUS.QUEUE
        ]

    def failed_jobs(self):
        return [ node.data for node in self.__ptree.nodes() if node.status == JOB_STATUS.FAILED ]

    def failed_job_roots(self):
        return [
            node.data
            for node in self.__ptree.nodes()
            if node.error is not None and not isinstance(node.error, ParentJobException)
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

    def jobs(self):
        return [ node.data for node in self.__ptree.nodes() ]

    def reset_jobs(self):
        for job_node in self.__ptree.nodes():
            job_node.status = JOB_STATUS.QUEUE

    def clear_jobs(self):
        return self.__ptree.clear_nodes()
