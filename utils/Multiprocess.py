import abc
import os
import multiprocessing as mp
import time
import sys


from math import ceil


class MultipleProcessRunner:
	"""
	Abstarct class for running tasks with multiple process
	"""
	
	@abc.abstractmethod
	def __init__(self, data, path, n_process=1):
		"""
		Args:
			data     : data to be processed that can be sliced
			path     : final output path
			n_process: number of process
		"""
		self.data = data
		self.path = path
		self.n_process = n_process
		self.cnt = mp.Value('d', 0)
		# get terminal width to format output
		# self.terminal_y = os.get_terminal_size()[0]
	
	def _s2hms(self, seconds: float):
		"""
		convert second format of time into hour:minute:second format

		"""
		m, s = divmod(seconds, 60)
		h, m = divmod(m, 60)
		
		return "%02d:%02d:%02d" % (h, m, s)
	
	def _display_time(self, st_time, now, total):
		ed_time = time.time()
		running_time = ed_time - st_time
		rest_time = running_time * (total - now) / now
		iter_sec = f"{now / running_time:.2f}it/s" if now > running_time else f"{running_time / now:.2f}s/it"
		
		return f' [{self._s2hms(running_time)} < {self._s2hms(rest_time)}, {iter_sec}]'
	
	def _display_all(self, now, total, desc, st_time):
		# make a progress bar
		length = 50
		now = now if now <= total else total
		num = now * length // total
		progress_bar = '[' + '#' * num + '_' * (length - num) + ']'
		display = f'{desc} {progress_bar} {int(now / total * 100):02d}% {now}/{total}'
		
		if st_time is not None:
			display += self._display_time(st_time, now, total)
		
		# clean a line
		display += ' ' * (self.terminal_y - len(display))
		
		# set color
		display = f"\033[31m{display}\033[0m"
		
		return display
	
	# Print progress bar at specific position
	def specific_progress_bar(self,
	                          process_id: int,
	                          now: int,
	                          total: int,
	                          desc: str = '',
	                          st_time: time.time = None):
		"""

		Args:
			process_id: process id
			now: now iteration number
			total: total iteration number
			desc: description
			st_time: if not None, display running time and the rest of time

		"""
		
		# aggregate total information
		self.cnt.value += 1
		if process_id == 0:
			total_display = self._display_all(int(self.cnt.value), self.__len__(), f"{self.path}: ", st_time)
			sys.stdout.write(f"\x1b7\x1b[{self.n_process + 1};{0}f{total_display}\x1b8")
		
		process_display = self._display_all(now, total, desc, st_time)
		sys.stdout.write(f"\x1b7\x1b[{process_id + 1};{0}f{process_display}\x1b8")
		sys.stdout.flush()
	
	def run(self, *args, **kwargs):
		"""
		The function is used to run a multi-process task

		Args:
			args: extra arguments to be input to function '_target' in addition to 'data' and 'sub_path'

		Returns: return the result of function '_aggregate()'

		"""
		num_per_process = ceil(self.__len__() / self.n_process)
		file_name, suffix = os.path.splitext(self.path)
		
		process_list = []
		sub_paths = []
		for i in range(self.n_process):
			st = i * num_per_process
			ed = st + num_per_process
			
			# construct slice and sub path for sub process
			data_slice = self.data[st: ed]
			sub_path = f"{file_name}_{i}{suffix}"
			input_args = (i, data_slice, sub_path, *args)
			
			p = mp.Process(target=self._target, args=input_args, kwargs=kwargs)
			p.start()
			
			process_list.append(p)
			sub_paths.append(sub_path)
		
		for p in process_list:
			p.join()
		
		# aggregate results
		return self._aggregate(self.path, sub_paths)
	
	@abc.abstractmethod
	def _aggregate(self, final_path: str, sub_paths):
		"""
		This function is used to aggregate results from sub processes into a file

		Args:
			final_path: path to save final results
			sub_paths : list of sub paths

		Returns: None or desirable results specified by user

		"""
		return
	
	@abc.abstractmethod
	def _target(self, process_id, data, sub_path, *args):
		"""
		The main body to operate data in one process

		Args:
			i       : process id
			data    : data slice
			sub_path: sub path to save results
			*args   : specific arguments for different tasks

		"""
		return
	
	@abc.abstractmethod
	def __len__(self):
		return