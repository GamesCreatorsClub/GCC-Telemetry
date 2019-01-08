import os

if not os.path.exists('example-fifo'):
    os.mkfifo("example-fifo")

pipe_fd = os.open('example-fifo', os.O_NONBLOCK | os.O_RDONLY)

pipe = os.fdopen(pipe_fd, 'rb', buffering=0)

while True:
    d = pipe.read(10)
    if d != b'' and d is not None:
        print("Got " + str(d))
