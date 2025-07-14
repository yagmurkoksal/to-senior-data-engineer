**Overall**

A simple way to describe the purpose of using DBT is to say that it handles the transformation step in the ELT process. What sets it apart from just running regular SQL is that it allows us to build modular structures, similar to those in software engineering. It greatly simplifies our work when it comes to managing dependencies, tests, and documentation. DBT itself is not used for moving data.

**Modules**

Modules are the basic blocks of DBT. There are certain features that distinguish models from ordinary SQL scripts. Models can reference each other and include macros, which makes it easier to manage dependencies effectively.

**Materializations**

Materializations are used to define how our models will be stored in the data warehouse. There are four default types:

    View: creates a new view.
    Table: rebuilds a table.
    Incremental: inserts or updates records.
    Ephemeral: not directly built in warehouse, basicly an cte.
    Materialized: creates a new mv view.

**Seeds and Sources**

Seeds are CSV files stored in your dbt project that can be loaded as tables into your data warehouse. They're typically used for static reference data. On the other hand, data that already exists in the warehouse is referred to as a source in DBT.

**Snapshots**

This is a particularly useful feature for SCD (Slowly Changing Dimension) processes. We can add SQL files directly under the snapshots folder in the dbt project. Each file includes configuration settings, a snapshot Jinja template line, and a SELECT * clause.

**Tests**

We can have generic and single tests in dbt. 
Generic test are built-in tests in DBT. We can add generic tests to schema.yml file.

Singular tests are very basic, we can contain them under tests folder as sql files.

**Macros**

Macros are quite similar to functions. In dbt, they allow you to create reusable blocks of SQL code. We define them using Jinja templates.

**Documentation**

One of the best things about dbt is how it simplifies documentation. We can create both basic and complex documentation. For simple docs, we can add descriptions directly within the YAML files. A short description we provide is used to generate an HTML link.
To create more complex documentation, we can use Markdown (.md) files, Jinja templates, and the docs function.

**Analyses and Hooks**

When we want to use dbt's macros or reference functionality without creating any materialized outputs, the analyses folder is the right place to store those SQL files.
Hooks are sql files which exexuted at predefined times.

    on_run_start: executed at start of dbt.
    on_run_end: executed at end of dbt.
    pre_hook: executed before a model built.
    post_hook: executed after a model built.

**Variables**

Dbt supports two types of variables: dbt-defined variables, which are passed through the vars configuration, and Jinja variables, which are defined directly within the template using Jinja syntax.

