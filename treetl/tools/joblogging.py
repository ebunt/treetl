
# a few helpers to get the log message structure out of the main job code


def kwargs_as_pretty_string(delim='=', **kwargs):
    if kwargs:
        return ', '.join([ '{}{}{}'.format(k, delim, v) for k, v in kwargs.items() ])
    else:
        return ''


def _name(job):
    return job.__class__.__name__


class JobLogger(object):
    def __init__(self, logger):
        self.logger = logger

    def log_method(self, job, method, inp_kwargs=None, other_info=None, **kwargs):
        self.logger.debug(
            msg='{}.{}({}){}'.format(
                _name(job),
                method,
                kwargs_as_pretty_string(**inp_kwargs) if inp_kwargs else '',
                ' | ' + kwargs_as_pretty_string(':', **other_info) if other_info else '',
            ),
            **kwargs
        )


class JobRunnerLogger(object):
    def __init__(self, logger):
        self.prefix = 'JobRunner: '
        self.logger = logger

    def log_status(self, status, **kwargs):
        from treetl.job import JOB_STATUS
        self.logger.info(self.prefix + 'JOB_STATUS.{}'.format(JOB_STATUS.Name[status]), **kwargs)

    def start_job(self, job):
        self.logger.info(self.prefix + 'Running {}'.format(_name(job)))

    def completed_job(self, job):
        self.logger.info(self.prefix + 'Completed {}'.format(_name(job)))

    def job_error(self, job):
        self.logger.error(
            self.prefix + 'Error on {}'.format(_name(job)),
            exc_info=True
        )

    def skip_job(self, job, parent):
        self.logger.info(self.prefix + 'Skipped {} due to failure in parent {}'.format(
            _name(job),
            _name(parent)
        ))

    def add_jobs(self, job, parents):
        self.logger.debug(self.prefix + 'Adding job {} with {} parent(s)'.format(
            job.__class__.__name__, len(parents)
        ))

    def log_job_method(self, *args, **kwargs):
        JobLogger(self.logger).log_method(*args, **kwargs)
