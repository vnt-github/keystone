
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes


class Mapper(api.Mapper):

    def map(self, context):
        for w in context.value.split():
            context.emit(w, 1)


class Reducer(api.Reducer):

    def reduce(self, context):
        context.emit(context.key, sum(context.values))


FACTORY = pipes.Factory(Mapper, reducer_class=Reducer)


def main():
    pipes.run_task(FACTORY)

# NOTE: __main__() needs to be defined.
def __main__():
    main()

if __name__ == "__main__":
    main()

# pydoop submit --upload-file-to-cache wc_submit_example.py wc_submit_example /wordcount/input /wordcount/wc_submit