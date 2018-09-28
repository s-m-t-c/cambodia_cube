from datacube_stats import StatsApp
from datacube_stats.statistics import GeoMedian
from datacube import Datacube
import yaml
from datacube_stats.utils import pickle_stream, unpickle_stream
import multiprocessing

import xarray as xr

def save_tasks():
    with open('stats_config.yaml') as fl:
        config = yaml.load(fl)

    print(yaml.dump(config, indent=4))

    print('generating tasks')
    dc = Datacube(app='api-example', config='cambodia.conf')
    app = StatsApp(config, index = dc.index)
    pickle_stream(app.generate_tasks(dc.index), 'task.pickle')

def execute_tasks(quartile):
    with open('stats_config.yaml') as fl:
        config = yaml.load(fl)

    print(yaml.dump(config, indent=4))

    task_file = f'task_q{quartile}.pickle'

    print('executing tasks')
    app = StatsApp(config)

    p = multiprocessing.Pool()
    p.map(app.execute_task, list(unpickle_stream(task_file)))

#    for x in generator:
#        perform(x)
#    map(perform, generator)
#    [perform(x) for x in generator]

#    for task in unpickle_stream(task_file):
#        print('executing', task)
#        app.execute_task(task)

def transform_tile(tile, q_dates):
    [num_observations] = tile.sources.time.shape

    sources = []

    for i in range(num_observations):
        one_slice = tile.sources.isel(time=slice(i, i + 1))
        if one_slice.time.astype('datetime64[M]') in q_dates.time.values.astype('datetime64[M]'):
            sources.append(one_slice)

    tile.sources = xr.concat(sources, dim='time')

    return tile

def transform_source(source, q_dates):
    try:
        source.data = transform_tile(source.data, q_dates)
        source.masks = [transform_tile(mask, q_dates) for mask in source.masks]
    except ValueError:
        return None

    return source

def transform_task(task, q_dates, quartile):
    # fix output file path to include the quartile
    new_path = 'nbart_geomedian_q' + str(quartile) + '_{x}_{y}.nc'
    task.output_products['ls_level2_geomedian_annual'].file_path_template = new_path

    # prune by the quartile dates
    task.sources = [transform_source(source, q_dates) for source in task.sources]
    task.sources = [source for source in task.sources if source is not None]

    if task.sources == []:
        return None

    return task

def prune_tasks(quartile):
    spei_q_dates = xr.open_dataset(f"spei_q{quartile}_dates.nc")

    pruned = (transform_task(task, spei_q_dates, quartile)
              for task in unpickle_stream('task.pickle'))
    pruned = (task for task in pruned if task is not None)

    pickle_stream(pruned, f'task_q{quartile}.pickle')

if __name__ == '__main__':
    save_tasks()            
    for i in [1, 4]:
        prune_tasks(i)
        execute_tasks(i)

