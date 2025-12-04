import time
class Timer:
    def __enter__(self): self.start=time.time(); return self
    def __exit__(self,*a): self.end=time.time()
    @property
    def seconds(self): return self.end-self.start
