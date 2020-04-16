voice-corpora-automation
========================

Automation to improve the dataset export process for Common Voice

Usage
-----
.. code-block:: shell

    $ voice-corpora-automation


Installation
------------
.. code-block:: shell

    $ python setup.py build
    $ python setup.py install

Configuration
-------------

Environment variables

* CV_DATABASE_URL
    * Common voice read-only replica
* CV_EXPORT_DIR
    * Path to store the clips tsv
* CV_S3_BUCKET
    * CV clips S3 bucket
* CORPORA_EXPORT_DIR
    * Path to store the corpora tsv
* CORPORA_DATABASE_URL
    * Path to the corpora database
* CORPORA_DATABASE_TABLE
    * Corpora database table
* CORPORA_S3_BUCKET
    * S3 bucket to store the public clips

Licence
-------
The MIT License (MIT)

Authors
-------

`voice-corpora-automation` was written by `John Giannelos <jgiannelos@mozilla.com>`_.
