# Telemetry-Framework
SERRANO Telemetry Framework

## Description

The SERRANO telemetry framework provides mechanisms to automatically collect inventory and monitoring information from heterogenous edge, cloud, and HPC platforms along with the deployed cloud-native applications and on-demand executed SERRANO-accelerated kernels. 

The framework includes a number of services written in Python and was impemented in the context of [SERRANO](https://ict-serrano.eu) Horizon 2020 project.

It consists of four key building blocks: (a) the Central Telemetry Handler, (b) the Enhanced Telemetry Agents, (c) the Monitoring Probes, and (d) the Persistent Monitotring Data Storage.

There are available three different monitoring probes: (a) the Kubernetes Monitoring Probe, (b) the HPC Monitoring Probe, and (c) the SERRANO Edge Devices Monitoring Probe.

## Requirements
The required dependencies are found in the `requirements.txt` file in each telemetry component. To install them, run `pip install -r requirements.txt` from the project folder.


## Additional information

More details are available in SERRANO Deliverables D5.3 (M15) and D5.4 (M31) in the [SERRANO project](https://ict-serrano.eu/deliverables/) web site.


