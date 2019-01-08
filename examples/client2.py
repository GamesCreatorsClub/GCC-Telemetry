import os, time

if not os.path.exists('example-fifo'):
    os.mkfifo("example-fifo")

pipe_fd = os.open('example-fifo', os.O_NONBLOCK | os.O_WRONLY)

pipe = os.fdopen(pipe_fd, 'wb', buffering=0)

now = time.time()
while time.time() - now < 20:
    pipe.write(bytes('ABCDEFGHIJ', 'ascii'))