
import logging
from treetl.tools.joblogging import JobLogger

job_logger = JobLogger(logging.getLogger(__name__))


_job_methods = [ 'extract', 'transform', 'load', 'cache', 'uncache' ]


class JobPatchMeta(type):
    """
    Metaclass for job patches that creates static version of ETL-CU methods. The static methods are named
    `static_[method]` and plugged into the corresponding method in the job being patched.
    """
    def __new__(cls, name, bases, dict):
        for m in _job_methods:
            if m in dict:
                dict['static_' + m] = staticmethod(dict[m])

        return super(JobPatchMeta, cls).__new__(cls, name, bases, dict)


# defined this way instead of with metaclass hooks for 2.x and 3.x portability
JobPatch = JobPatchMeta(str('JobPatch'), (), {})


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
    def inject(*args):
        """
        Apply job patches without messing with the MRO. Extends the main definitions of extract, transform, load,
        cache, and uncache. Adds all non-double underscore attributes and methods from injected classes.

        In short, inheritance without MRO changes, extra bases, derived class et c.
        :param args: Patches to be applied
        :return: The decorated class with attributes added/extended
        """
        def class_wrap(cls):
            def new_by_type(f_type):
                orig = getattr(cls, f_type)
                def new_func(self, **kwargs):
                    orig(self, **kwargs)
                    for inj in args:
                        if hasattr(inj, 'static_' + f_type):
                            getattr(inj, 'static_' + f_type)(self, **kwargs)
                    return self
                return new_func

            d = cls.__dict__.copy()
            d.update({
                m: new_by_type(m)
                for m in _job_methods
            })

            # build new init
            orig_init = cls.__init__
            def new_init(self, *init_args, **kwargs):
                orig_init(self, *init_args, **kwargs)

                # create patch jobs and append class methods and attributes to base job
                for a in args:
                    try:
                        next_patch = a(**kwargs)
                    except:
                        next_patch = a()

                    for a_name in dir(next_patch):
                        if a_name not in _job_methods and 'static_' not in a_name and not a_name.startswith('__'):
                            setattr(cls, a_name, getattr(next_patch, a_name))

            # make sure __init__ and ETL-CU are appended
            cls.__init__ = new_init
            for attr in _job_methods:
                setattr(cls, attr, d[attr])
            return cls

        return class_wrap

    @staticmethod
    def extractors(**kwargs):
        """
        Add extractors to a job. These are functions that do not take self.
        :param kwargs: name_of_attribute_to_store_data_in = extractor_function
        :return: wrapped class with the appended extractors
        """
        def class_wrap(cls):
            orig_f = getattr(cls, 'extract')
            def new_function(self, **nf_kwargs):
                # call parent extract
                orig_f(self, **nf_kwargs)
                for k, v in kwargs.items():
                    setattr(self, k, v(**nf_kwargs))
                return self

            return type(cls.__name__, (cls,), { 'extract': new_function }
            )
        return class_wrap

    @staticmethod
    def transformers(*args):
        """
        Add basic transformers to a job. These are functions that do not take self.
        :param args: function w signature f(data_to_be_transformed, **kwargs) that returns post transform data
        :return: wrapped class with the appended transformers
        """
        def class_wrap(cls):
            orig_f = getattr(cls, 'transform')
            def new_function(self, **nf_kwargs):
                # call parent transform
                orig_f(self, **nf_kwargs)

                # if original transformer did anything get the transformed_data
                next_data = getattr(self, 'transformed_data')
                if next_data is None:
                    # otherwise the first *args transformer should start with extracted_data
                    next_data = getattr(self, 'extracted_data')

                for a in args:
                    setattr(self, 'transformed_data', a(next_data, **nf_kwargs))
                    next_data = getattr(self, 'transformed_data')
                return self

            return type(cls.__name__, (cls,), { 'transform': new_function })
        return class_wrap

    @staticmethod
    def create(job_name, extract=None, transform=None, load=None, cache=None, uncache=None, **kwargs):
        def as_job_m(m, attr, prior_attr=None):
            if m is not None:
                def wrapped(self, **kwargs):
                    if prior_attr is not None:
                        kwargs[prior_attr] = getattr(self, prior_attr)
                    res = m(**kwargs)
                    if attr is not None:
                        setattr(self, attr, res)
                    return self
                return wrapped
            else:
                return lambda self, **kwargs: self

        new_job_type = type(job_name, (Job,), {
            'extract': as_job_m(extract, 'extracted_data'),
            'transform': as_job_m(transform, 'transformed_data', 'extracted_data'),
            'load': as_job_m(load, None, 'transformed_data'),
            'cache': as_job_m(cache, None, 'transformed_data'),
            'uncache': as_job_m(uncache, None, 'transformed_data')
        })

        return Job.dependency(**kwargs)(new_job_type) if kwargs else new_job_type

    def __init__(self, **kwargs):
        self.extracted_data = None
        self.transformed_data = None

    def extract(self, **kwargs):
        job_logger.log_method(self, 'extract', inp_kwargs=kwargs)
        return self

    def transform(self, **kwargs):
        job_logger.log_method(self, 'transform', inp_kwargs=kwargs)
        return self

    def load(self, **kwargs):
        job_logger.log_method(self, 'load', inp_kwargs=kwargs)
        return self

    def cache(self, **kwargs):
        job_logger.log_method(self, 'cache', inp_kwargs=kwargs)
        return self

    def uncache(self, **kwargs):
        job_logger.log_method(self, 'uncache', inp_kwargs=kwargs)
        return self
