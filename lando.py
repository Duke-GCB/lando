from time import sleep


def image_list():
    print 'listing images'
    return ['ubuntu']


def launch_instance(image):
    print 'launching image {} to instance'.format(image)
    return 'instance of {}'.format(image)


def terminate_instance(instance):
    print 'terminating instance {}'.format(instance)
    pass


def main():
    images = image_list()
    if len(images) == 0:
        return
    image = images[0]
    instance = launch_instance(image)
    sleep(5)
    terminate_instance(instance)


if __name__ == '__main__':
    main()