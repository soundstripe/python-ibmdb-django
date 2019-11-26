import random
import string

SECRET_KEY = [random.choice(string.ascii_lowercase) for i in range(10)]

DATABASES = {
    'default':
        {
            'ENGINE': 'iseries',
            'NAME': 'test_iseries'
        }
}