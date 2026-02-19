import ray
import time

@ray.remote
def say_hello():
    time.sleep(5)
    return "Hello from Anyscale!"

if __name__ == "__main__":
    ray.init()
    print(ray.get(say_hello.remote()))
