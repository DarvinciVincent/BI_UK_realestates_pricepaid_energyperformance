import importlib


def main(pipeline_name, pipeline_stage):    

    pipeline_name = ".".join(["pipelines", pipeline_name, pipeline_stage])
    try:
        pipeline = importlib.import_module(pipeline_name)
        pipeline.main()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    import sys
    main(*sys.argv[1:])