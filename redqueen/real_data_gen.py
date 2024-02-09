import broadcast.data.user_repo    as user_repo
import broadcast.data.db_connector as db_connector
import numpy                       as np
import decorated_options           as Deco
import broadcast.data.hdfs         as hdfs
import logging
import warnings

# Comment out while running from IPython to avoid reloading the module
try:
    from opt_model import RealData, SimOpts
    from utils import is_sorted, logTime, def_q_vec
except NameError:
    # The may have been imported using %run -i directly.
    warnings.warn('Unable to import opt_model and utils.')


scaled_period = 10000.0
verbose = False


def get_start_end_time():
    return Deco.Options(
        start_time = 1246406400, # GMT: Wed, 01 Jul 2009 00:00:00 GMT
        end_time = 1251763200 # GMT: Tue, 01 Sep 2009 00:00:00 GMT
    )

log = logTime if verbose else lambda *args, **kwargs: None

def get_user_repository():
    """Generates the user-repository for a user."""
    try:
        if os.path.isfile('/dev/shm/db.sqlite3') and os.path.isfile('/dev/shm/links.sqlite3'):
            conn = db_connector.DbConnection(db_path='/dev/shm/db.sqlite3',
                                             link_path='/dev/shm/links.sqlite3')
        else:
            raise IOError()
    except (OSError, IOError):
        logging.warning('The SQLite files not found on /dev/shm, looking for them on local drives.')
        try:
            if os.path.isfile('/local/moreka/db.sqlite3') and os.path.isfile('/local/moreka/links.sqlite3'):
                conn = db_connector.DbConnection(db_path='/local/moreka/db.sqlite3',
                                                 link_path='/local/moreka/links.sqlite3')
            else:
                raise IOError()
        except (OSError, IOError):
            try:
                if os.path.isfile('/local/utkarshu/db.sqlite3') and os.path.isfile('/local/utkarshu/links.sqlite3'):
                     conn = db_connector.DbConnection(db_path='/local/utkarshu/db.sqlite3',
                                                     link_path='/local/utkarshu/links.sqlite3')
                else:
                    raise IOError()
            except (OSError, IOError):
                raise IOError('The twitter DBs were not found.')


    try:
        hdfs_loader = hdfs.HDFSLoader('/dev/shm/tweets_all.h5')
    except (OSError, IOError):
        logging.warning('The HDF5 file not found on /dev/shm, looking for them on local drives.')
        try:
            hdfs_loader = hdfs.HDFSLoader('/local/moreka/tweets_all.h5')
        except (OSError, IOError):
            try:
                hdfs_loader = hdfs.HDFSLoader('/local/utkarshu/tweets_all.h5')
            except (OSError, IOError):
                raise IOError('The HDF5 DB was not found.')

    return user_repo.HDFSSQLiteUserRepository(hdfs_loader, conn)


def scale_times(ts, start_time, end_time, T=None):
    if T is None:
        T = scaled_period

    s = 1.0 * (end_time - start_time) / T
    return [(t - start_time) / s
            for t in ts
            if start_time <= t <= end_time]

def make_real_data_broadcaster(src_id, wall, start_time, end_time, T=None):
    return RealData(src_id, scale_times(wall, start_time, end_time, T))


def calc_avg_user_intensity(user_tweets, start_time, end_time, T=None):
    if T is None:
        T = scaled_period

    return len([x for x in user_tweets
                if start_time <= x and x <= end_time]) / T

# def run_for_user(user_id):
#     """Runs the optimization procedure treating user_id as the broadcaster
#        being replaced."""

# user_id = 1004 # 27 minutes
user_id = 12223582 # 4 minutes

# Generating the connection and the HDFS loader again
# To make sure that the loader is thread-safe.


def get_user_data_for(user_id):
    hs = get_user_repository()

    try:
        user_tweet_times = hs.get_user_tweets(user_id)

        assert is_sorted(user_tweet_times), "User tweet times were not sorted."
        # TODO: This should ideally be the last 2 months instead of tweeting history of
        # the broadcaster. Or ....
        # Is 2 months now.

        first_tweet_time, last_tweet_time = user_tweet_times[0], user_tweet_times[-1]
        exp_times = get_start_end_time()
        start_time = exp_times.start_time # GMT: Wed, 01 Jul 2009 00:00:00 GMT
        end_time = exp_times.end_time # GMT: Tue, 01 Sep 2009 00:00:00 GMT

        if last_tweet_time < start_time:
            # This user did not tweet in the relevant period of time
            return (user_id, None)

        # user_intensity = calc_avg_user_intensity(user_tweet_times,
        #                                          start_time,
        #                                          end_time)


        # These are the ids of the sink
        user_followers = hs.get_user_followers(user_id)
        user_relevant_followers = []
        log("Num followers of {} = {}".format(user_id, len(user_followers)))
        edge_list, sources = [], []

        for idx, follower_id in enumerate(user_followers):
            followees_of_follower = len(hs.get_user_followees(follower_id))
            if followees_of_follower < 500:
                # If the number of followees of the follower are > 500, then Do not
                # create their walls. This will discard about 30% of all users.
                user_relevant_followers.append(follower_id)
                wall = hs.get_user_wall(follower_id, excluded=user_id)
                follower_source = make_real_data_broadcaster(follower_id, wall,
                                                             start_time,
                                                             end_time)
                log("Wall of {} ({}/{}) has {} tweets, {} relevant"
                    .format(follower_id, idx + 1, len(user_followers),
                            len(wall), follower_source.get_num_events()))
                # The source for follower_id has the same ID
                # There is one source per follower which produces the tweets
                sources.append(follower_source)
                edge_list.append((follower_id, follower_id))

        # The source user_id broadcasts tweets to all its followers
        edge_list.extend([(user_id, follower_id)
                          for follower_id in user_relevant_followers])

        other_source_params = [('RealData', {'src_id': x.src_id,
                                             'times': x.times})
                               for x in sources]

        sim_opts = SimOpts(edge_list=edge_list,
                       sink_ids=user_relevant_followers,
                       src_id=user_id,
                       q_vec=def_q_vec(len(user_relevant_followers)),
                       s=1.0,
                       other_sources=other_source_params,
                       end_time=scaled_period)

        return (user_id, (sim_opts,
                          scale_times(user_tweet_times, start_time, end_time, scaled_period)))
    except Exception as e:
        print('Encountered error', e, ' for user {}'.format(user_id))
        return user_id, None
    finally:
        hs.close()


@Deco.optioned('opts')
def find_significance(user_id,
                      user_repository,
                      num_segments=24,
                      segment_length=60*60,
                      return_tweet_times=False):
    # 1. Find all the followers
    # 2. Find tweet times of the followers
    # 3. Fit them in num_segments - per day.

    experiment_times = get_start_end_time()
    start_time = experiment_times.start_time # GMT: Wed, 01 Jul 2009 00:00:00 GMT

    followee_threshold = 500

    user_followers = user_repository.get_user_followers(user_id)

    if len(user_followers) > followee_threshold:
        logging.error('Number of followers is more than 500.')
        return None

    follower_significance = []
    all_tweet_times = []

    time_period = num_segments * segment_length

    # Not sorting the users here to keep the same order as was recorded for the user initially
    for idx, follower_id in enumerate(user_followers):
        num_followees_of_follower = len(user_repository.get_user_followees(follower_id))

        if num_followees_of_follower < followee_threshold:
            follower_tweet_times = user_repository.get_user_tweets(follower_id)
            # Only use times before start_time
            follower_tweet_times = follower_tweet_times[follower_tweet_times < start_time]

            follower_tweet_bins = [0] * num_segments
            if return_tweet_times:
                all_tweet_times.append(follower_tweet_times)

            for tweet_time in follower_tweet_times:
                idx = int(num_segments * ((tweet_time - start_time) % time_period) / time_period)
                follower_tweet_bins[idx] += 1

            follower_significance.append(follower_tweet_bins)

    raw_significance=np.asarray(follower_significance)
    total_raw_significance = raw_significance.sum(1)
    significance = raw_significance / total_raw_significance[:,None]
    avg_for_others = np.nanmean(significance, axis=0)
    # Fill in the NaNs with the average for the other followers, who have
    # at least one follower.
    significance[total_raw_significance == 0, :] = avg_for_others

    # Now if there are any NaNs still left (i.e. if nobody tweeted anything)
    significance[np.isnan(significance)] = 1.0 / num_segments

    ret = Deco.Options(
        raw_significance=raw_significance,
        significance=significance,
        total_followers=len(user_followers)
    )

    if return_tweet_times:
        ret = ret.set_new(all_tweet_times=all_tweet_times)

    return ret


# ------------------------------------------------------------------------

output_folder = '/NL/ghtorrent/work/opt-broadcast/'

import multiprocessing as mp
import pandas as pd
import os
import seqfile
import pickle


def make_user_file_name(user_id):
    return os.path.join(output_folder, 'user-{}.pickle'.format(user_id, scaled_period))

def save_user_setups(input_csv):
    df = pd.read_csv(input_csv)
    success = []
    fails = []

    new_user_ids = [int(x) for x in df.user_id
                    if not os.path.exists(make_user_file_name(x))]

    num_users = len(new_user_ids)
    logTime('Working on {}/{} users.'.format(num_users, df.shape[0]))

    with mp.Pool() as pool:
        count = 0
        for user_id, res in pool.imap_unordered(get_user_data_for, new_user_ids):
            count += 1
            logTime('Done {}/{}'.format(count, num_users))

            if res is None:
                logTime('User_id {} had no tweets in experiment period.'.format(user_id))
                fails.append(user_id)
            else:
                success.append(user_id)
                f_name = make_user_file_name(user_id)
                sim_opts, user_event_times = res
                try:
                    with open(f_name, 'wb') as pickle_file:
                        pickle.dump({
                                'sim_opts_dict': sim_opts.get_dict(),
                                'user_event_times': user_event_times,
                                'num_user_events': len(user_event_times),
                                'user_id': user_id,
                                'scaled_period': scaled_period
                            }, pickle_file)
                except:
                    # If there was an exception, then remove the file to
                    # not skip it on the next round. Then re-raise the error.
                    try:
                        os.remove(f_name)
                        logTime('Removed {}'.format(f_name))
                    except OSError:
                        pass

                    raise

                logTime('User_id {} had {} tweets in experiment period.'
                        .format(user_id, len(user_event_times)))

    logTime('Done.')
    return (success, fails)

log("All loaded.")

# Step 1: Select 10k random broadcastors
# Step 2: Find their followers
# Step 3: Find the walls of the followers
# Step 4: Save the walls in a format which a Broadcaster can read later
#  - Why not generate it on the fly? Because we will save only the walls of the
#    last N months?
#  - Why not do the experiment on the largest scale possible?
