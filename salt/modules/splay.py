'''
Splay function calls across targeted minions
'''
import time
import numpy as np
from salt.exceptions import CommandExecutionError


_DEFAULT_SPLAYTIME = 300
_DEFAULT_SIZE = 8192


def _get_hash(hashable, size):
    '''
    Jenkins One-At-A-Time Hash Function
    More Info: http://en.wikipedia.org/wiki/Jenkins_hash_function#one-at-a-time
    '''
    h = np.uint32(0)
    for i in bytearray(hashable):
        h += np.uint32(i)
        h += np.uint32(h << 10)
        h ^= np.uint32(h >> 6)

    h += np.uint32(h << 3)
    h ^= np.uint32(h >> 11)
    h += np.uint32(h << 15)

    return np.uint32(h & (size - 1))


def _calc_splay(hashable, splaytime=_DEFAULT_SPLAYTIME, size=_DEFAULT_SIZE):
    hash_val = _get_hash(hashable, size)
    return int(splaytime * hash_val / float(size))


def splay(*args, **kwargs):
    '''
    Splay a salt function call execution time across minions over
    a number of seconds (default: 300)


    NOTE: You *probably* want to use --async here and look up the job results later.
          If you're dead set on getting the output from the CLI command, then make
          sure to set the timeout (with the -t flag) to something greater than the
          splaytime (max splaytime + time to execute job). 
          Otherwise, it's very likely that the cli will time out before the job returns.


    CLI Example:
    # With default splaytime
      salt --async '*' splay.splay pkg.install cowsay version=3.03-8.el6

    # With specified splaytime (10 minutes) and timeout
      salt -t 610 '*' splay.splay 600 pkg.version cowsay
    '''
    # Convert args tuple to a list so we can pop the splaytime and func out
    args = list(args)

    # If the first argument passed is an integer, set it as the splaytime
    try:
        splaytime = int(args[0])
        args.pop(0)
    except ValueError:
        splaytime = _DEFAULT_SPLAYTIME

    if splaytime <= 0:
        raise ValueError('splaytime must be a positive integer')

    func = args.pop(0)
    # Check if the func is valid before the sleep
    if not func in __salt__:
        raise CommandExecutionError('Unable to find module function {0}'.format(func))


    my_delay = _calc_splay(__grains__['id'], splaytime=splaytime)
    time.sleep(my_delay)
    # Get rid of the hidden kwargs that salt injects
    func_kwargs = dict((k,v) for k,v in kwargs.iteritems() if not k.startswith('__'))
    result = __salt__[func](*args, **func_kwargs)
    if type(result) != dict:
        result = {'result': result}
    result['splaytime'] = str(my_delay)
    return result


def show(splaytime=_DEFAULT_SPLAYTIME):
    '''
    Show calculated splaytime for this minion
    Will use default value of 300 (seconds) if splaytime value not provided


    CLI Example:
        salt example-host splay.show
        salt example-host splay.show 60
    '''
    # Coerce splaytime to int (passed arg from CLI will be a str)
    if type(splaytime) != int:
        splaytime = int(splaytime)

    return str(_calc_splay(__grains__['id'], splaytime=splaytime))
