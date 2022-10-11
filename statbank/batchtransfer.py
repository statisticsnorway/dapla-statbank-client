from datetime import datetime as dt

from .auth import StatbankAuth  # Needed for inheritance
from .transfer import StatbankTransfer  # Needed for typecheck


class StatbankBatchTransfer(StatbankAuth):
    """
    Takes a bunch of delayed Transfer jobs in a list, so they can all be dispatched at the same time, with one password request.
        ...
    
    Attributes
    ----------    
    jobs: 
        list of delayed StatbankTransfers.
    lastebruker:
        extracts the "lastebruker" from the first transfer-job. 
        All jobs in the batch-transfer must use the same user and password.
    headers:
        Deleted without warning. Temporarily holds the headers to Authenticate the transfers.
    
    Methods
    -------
    transfer():
        Builds headers (asks for password), calls the transfer function of each StatbankTransfer-job. Deletes headers.
    
    """    
    def __init__(self,
                jobs: list = []):
        if not jobs:
            raise ValueError("Can't batch-transfer, no transfers, give me a list of delayed StatbankTransfers.")
        self.jobs = jobs
        # Make sure all jobs are delayed StatbankTransfer-objects
        for i, job in enumerate(self.jobs):
            if not isinstance(job, StatbankTransfer):
                raise TypeError(f"Transfer-job {i} is not a StatbankTransfer-object.")
            if not job.delay:
                raise ValueError(f"Transfer-job {i} was not delayed?")
        self.lastebruker = self.jobs[0].lastebruker
        self.transfer()
        self.transfertime = dt.now().strftime("%Y-%m-%d")
        
    def transfer(self):
        self.headers = self._build_headers()
        try:
            for job in self.jobs:
                job.transfer(self.headers)
        finally: 
            del self.headers
            
    def __str__(self):
        return f'StatbankBatchTransfer with {len(self.jobs)} jobs, lastebruker: {self.lastebruker}, transferred at {self.transfertime}'
        
    def __repr__(self):
        transfers = []
        transfers += ["StatbankTransfer"]*len(self.jobs)
        return f'StatbankBatchTransfer({transfers})'