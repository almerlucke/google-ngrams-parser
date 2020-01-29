from multiprocessing import Pipe
from multiprocessing import Process


def run_parallel(jobs):
    """Run jobs in parallel using only pipe and process so it can also be used on AWS Lambda

    :param jobs: A list of jobs, each job is a tuple with a function and a tuple of args to pass
    :return: A list with all results of the jobs
    """

    # Wrapper to call function and perform conn send and close
    def run_one(conn, function, args):
        conn.send(function(*args))
        conn.close()

    processes = []
    results = []
    receivers = []

    # Start jobs
    for job in jobs:
        # Create new pipe
        receiver, sender = Pipe()

        # Add to receivers
        receivers.append(receiver)

        # Create, start and store process
        process = Process(target=run_one, args=(sender, job[0], job[1]))
        process.start()
        processes.append(process)

    # Listen for incoming results
    for receiver in receivers:
        try:
            # Get result and close receiver
            results.append(receiver.recv())
            receiver.close()
        except EOFError:
            receiver.close()

    # Join all processes when they are finished
    for process in processes:
        process.join()

    return results


def run_parallel_batches(jobs, batch_size, batch_callback=None):
    """Split jobs in batches and sequentially run the batches. The jobs in each batch
    are run in parallel but there is only one batch computed at the time.

    :param jobs: A list of jobs, each job is a tuple with a function and a tuple of args to pass
    :param batch_size: The batch size to split the jobs in batches
    :param batch_callback: An optional callback which is called with the batch results
    :return: A list with all the results of the batch jobs combined
    """
    results = []

    # Split in job batches
    for i in range(0, len(jobs), batch_size):
        # Run the batch in parallel
        batch_results = run_parallel(jobs[i: i + batch_size])
        results += batch_results

        # Call the callback
        if batch_callback:
            batch_callback(batch_results)

    return results
