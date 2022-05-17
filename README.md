# Constraints Analysis using historical NEM data in nempy

**Last Updated: 17/05/2022**

<br> This repo presents a series of constraints analysis case studies of historical NEM data using nempy. [Nempy](https://github.com/UNSW-CEEM/nempy) is a dispatch simulation tool used here to replicate the NEMDE process and rerun different market scenarios counterfactually. This package has further been adapted (see [forked nempy](https://github.com/dec-heim/nempy_constraints_v1)) such that constraints can be analysed by accessing variables such as the marginal value of binding constraints.

A key case study here presents an assessment of the proposed Congestion Relief Market policy using these constraint tools. This project was completed in the context of university coursework (SOLA5050 Renewable Energy Policy) - hence a disclaimer here.. it is not advice and should not be interpreted in any other such context.   

<br>Further files provided here are example notebooks exploring analysis of constraints and detailing methodology/comments within.

This repo may be updated with more examples and improvements to the constraints implementation within the forked version of nempy.
There are also aims to integrate this into the official release of nempy, once test have been validated etc.

**Current Examples**
- "Congestion Relief Market Study": See subfolder 'SOLA5050' and attached .pdf report.
- "example_a":              Investigates single dispatch interval constraint demonstrating constraints analysis functionality.
- "example_a_timeseries":   Follows the above example but across multiple dispatch intervals.
