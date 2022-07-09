synced 08.07.22 with mvfunctions

# Examples
look into example.py to see how async functions work in Python

# Whats the difference between multithreading and async task handling?

An analogy usually helps. You are cooking in a restaurant. An order comes in for eggs and toast.

* Synchronous: you cook the eggs, then you cook the toast.
* Asynchronous, single threaded: you start the eggs cooking and set a timer. You start the toast cooking, and set a timer. While they are both cooking, you clean the kitchen. When the timers go off you take the eggs off the heat and the toast out of the toaster and serve them.
* Asynchronous, multithreaded: you hire two more cooks, one to cook eggs and one to cook toast. Now you have the problem of coordinating the cooks so that they do not conflict with each other in the kitchen when sharing resources. And you have to pay them.


Now does it make sense that multithreading is only one kind of asynchrony? **Threading is about workers; asynchrony is about tasks.** In multithreaded workflows you assign tasks to workers. In asynchronous single-threaded workflows you have a graph of tasks where some tasks depend on the results of others; as each task completes it invokes the code that schedules the next task that can run, given the results of the just-completed task. But you (hopefully) only need one worker to perform all the tasks, not one worker per task.

Answer of: https://stackoverflow.com/questions/34680985/what-is-the-difference-between-asynchronous-programming-and-multithreading

