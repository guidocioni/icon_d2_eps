import numpy as np
from multiprocessing import Pool
from functools import partial
from utils import *
import sys

debug = False
if not debug:
    import matplotlib
    matplotlib.use('Agg')

import matplotlib.pyplot as plt

# The one employed for the figure name when exported 
variable_name = 'prob_snow'

print_message('Starting script to plot '+variable_name)

# Get the projection as system argument from the call so that we can 
# span multiple instances of this script outside
if not sys.argv[1:]:
    print_message(
        'Projection not defined, falling back to default (de)')
    projection = 'de'
else:
    projection = sys.argv[1]


def main():
    dset = read_dataset()[['SNOW_CON', 'SNOW_GSP', 'RAIN_CON', 'RAIN_GSP', 'run']]

    dset = compute_rate(dset)

    dset['prob_snow'] = ((dset.snow_rate > 0.25).sum(dim='realization') / 20) * 100
    dset['prob_rain'] = ((dset.snow_rate > 0.1).sum(dim='realization') / 20) * 100


    levels = np.linspace(10, 100, 10)

    cmap_snow, norm_snow = get_colormap_norm("snow", levels)
    cmap_rain, norm_rain = get_colormap_norm("rain", levels)

    _ = plt.figure(figsize=(figsize_x, figsize_y))

    ax = plt.gca()
    m, x, y = get_projection(dset, projection, labels=True)
    m.fillcontinents(color='lightgray', lake_color='whitesmoke', zorder=0)

    dset = dset.drop(['SNOW_CON', 'SNOW_GSP', 'RAIN_CON', 'RAIN_GSP',
                      'rain_rate', 'snow_rate']).load()

    # All the arguments that need to be passed to the plotting function
    args = dict(x=x, y=y, ax=ax, cmap_snow=cmap_snow, norm_snow=norm_snow,
                cmap_rain=cmap_rain, norm_rain=norm_rain, levels=levels)

    print_message('Pre-processing finished, launching plotting scripts')
    if debug:
        plot_files(dset.isel(time=slice(0, 2)), **args)
    else:
        # Parallelize the plotting by dividing into chunks and processes
        dss = chunks_dataset(dset, chunks_size)
        plot_files_param = partial(plot_files, **args)
        p = Pool(processes)
        p.map(plot_files_param, dss)


def plot_files(dss, **args):
    first = True
    for time_sel in dss.time:
        data = dss.sel(time=time_sel)
        time, run, cum_hour = get_time_run_cum(data)
        # Build the name of the output image
        filename = subfolder_images[projection] + '/' + variable_name + '_%s.png' % cum_hour

        cs = args['ax'].tricontourf(args['x'],
                                    args['y'],
                                    data['prob_rain'],
                                    extend='max',
                                    cmap=args['cmap_rain'],
                                    norm=args['norm_rain'],
                                    levels=args['levels'])

        css = args['ax'].tricontourf(args['x'],
                                    args['y'],
                                    data['prob_snow'],
                                    extend='max',
                                    cmap=args['cmap_snow'],
                                    norm=args['norm_snow'],
                                    levels=args['levels'])

        an_fc = annotation_forecast(args['ax'], time)
        an_var = annotation(args['ax'], 'Prob. snow rate > 0.25 mm/h & rain rate > 0.1 mm/h', loc='lower left', fontsize=6)
        an_run = annotation_run(args['ax'], run)
        logo = add_logo_on_map(ax=args['ax'], zoom=0.1, pos=(0.95, 0.08))

        if first:
            if projection == 'de':
                x_cbar_0, y_cbar_0, x_cbar_size, y_cbar_size     = 0.15, 0.05, 0.3, 0.02
                x_cbar2_0, y_cbar2_0, x_cbar2_size, y_cbar2_size = 0.55, 0.05, 0.3, 0.02
            elif projection == 'it':
                x_cbar_0, y_cbar_0, x_cbar_size, y_cbar_size     = 0.15, 0.2, 0.35, 0.02
                x_cbar2_0, y_cbar2_0, x_cbar2_size, y_cbar2_size = 0.55, 0.2, 0.35, 0.02
            elif projection == "nord":
                x_cbar_0, y_cbar_0, x_cbar_size, y_cbar_size     = 0.15, 0.09, 0.35, 0.02
                x_cbar2_0, y_cbar2_0, x_cbar2_size, y_cbar2_size = 0.55, 0.09, 0.35, 0.02 

            ax_cbar = plt.gcf().add_axes([x_cbar_0, y_cbar_0, x_cbar_size, y_cbar_size])
            ax_cbar_2 = plt.gcf().add_axes([x_cbar2_0, y_cbar2_0, x_cbar2_size, y_cbar2_size])
            cbar_snow = plt.gcf().colorbar(css, cax=ax_cbar, orientation='horizontal',
             label='Snow prob.')
            cbar_rain = plt.gcf().colorbar(cs, cax=ax_cbar_2, orientation='horizontal',
             label='Rain prob.')
            cbar_snow.ax.tick_params(labelsize=8) 
            cbar_rain.ax.tick_params(labelsize=8)

        if debug:
            plt.show(block=True)
        else:
            plt.savefig(filename, **options_savefig)

        remove_collections([cs, css, an_fc, an_var, an_run, logo])

        first = False


if __name__ == "__main__":
    import time
    start_time=time.time()
    main()
    elapsed_time=time.time()-start_time
    print_message("script took " + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
