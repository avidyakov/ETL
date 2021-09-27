from check import Checker
from process import Process
from state import JsonFileStorage, State


if __name__ == '__main__':
    storage = JsonFileStorage('state.json')
    state = State(storage)
    checkers = (Checker(state), )
    processor = Process(checkers)
    processor.start()
