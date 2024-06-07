Quick Start
###########

.. warning::

    This process is currently specifically set up to run on TfN's servers, with associated file paths.
    It may be possible to run in other locations, however this should be discussed with appropriate
    TfN staff first.

Environment set-up
==================

The first step is to copy the code and set up the environment. For this you will need Conda installed
on your machine (ideally Miniforge), with the following three lines 

.. code-block:: shell

    git clone https://github.com/Transport-for-the-North/Land-Use
    cd Land-Use
    conda env create -f environment.yml

Choosing an entry point
=======================

.. error::

    This section is incomplete, and should be updated as more code is written

Once the environment has been set up, you will need to choose an entry point depending on what you
are trying to accomplish:

- :code:`base_population.py` allows for rebuilding of base year population figures
- :code:`base_employment.py` allows for rebuilding of base year employment figures

This will create an environment called :code:`normits_lu` which will need to be activated before 
running any code.

Running a scenario
==================

Configuration
-------------

Standard scenario configuration files can be found in the :code:`scenario_configurations` directory.
In many cases, running one of these existing configurations will be sufficient, however 
modifications can be made if required (e.g. to swap input files)

