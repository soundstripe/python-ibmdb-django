# Getting started with IBM DB Django adapter 

IBM iSeries Db2 (pyodbc/iaccess driver) support for the Django application Framework

forked from python-ibmdb-django

# Prerequisites for Django for iSeries

 * Python 3.6+
 * Django Framework 2.2.x or above.
 * iAccess Client ODBC Driver
 
# Installation 

## 1. Install Django 

Install Django as per instructions from the Django [http://docs.djangoproject.com/en/dev/topics/install/#installing-an-official-release website].

## 2. Install pyodbc and the DB2 Django adapter (django-pyodbc-iseries)  

```  
$ pip install django-pyodbc-iseries  
```
 
# Tested Operating Systems 

 * Ubuntu Linux 18
 * Win64/Win32 bit
 * Mac OS

# Supported Databases 

 * IBM Db2 for iSeries

# Testing 
```
 * Create a new Django project by executing "django-admin.py startproject myproj".
 * Now go to this newly create directory, and edit settings.py file to access DB2.
 * In case of nix the steps will be like:
  {{{
  $ django-admin.py startproject myproj
  $ cd myproj
  $ vi settings.py
  }}}
 * The settings.py will be like (after adding DB2 properties):
   {{{
   DATABASES = {
      'default': {
         'ENGINE'     : 'iseries',
         'NAME'       : 'mydb',
         'USER'       : '<username>',
         'PASSWORD'   : '<pass>',
         'HOST'       : 'host/ipaddress',
      }
   }
   }}}
   
 * Change USE_TZ to False
 
 * RUN python manage.py migrate
 
 * In the tuple INSTALLED_APPS in settings.py add the following lines:
   {{{
   'django.contrib.flatpages',
   'django.contrib.redirects',
   'django.contrib.comments',
   'django.contrib.admin',
   }}}
 * Next step is to run a simple test suite. To do this just execute following command in the project we created earlier:
   {{{
   $ python manage.py test #for Django-1.5.x or older
   $ Python manage.py test django.contrib.auth #For Django-1.6.x onwards, since test discovery behavior have changed
   }}} 
 * For Windows, steps are same as above. In case of editing settings.py file, use notepad (or any other) editor.
```
# Database Transactions 

 *  Django by default executes without transactions i.e. in auto-commit mode. This default is generally not what you want in web-applications. [http://docs.djangoproject.com/en/dev/topics/db/transactions/ Remember to turn on transaction support in Django]

