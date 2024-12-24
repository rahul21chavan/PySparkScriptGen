/* SAS Script 1: Data Step Example */
data work.sales;
    set sashelp.sales;
    if region = 'North America' then total_sales = sum(sales, 100);
    else total_sales = sales;
run;

/* SAS Script 2: PROC PRINT Example */
proc print data=work.sales;
run;

/* SAS Script 3: PROC MEANS Example */
proc means data=sashelp.class mean;
    var height weight;
run;

/* SAS Script 4: PROC FREQ Example */
proc freq data=sashelp.cars;
    tables type origin;
run;

/* SAS Script 5: Data Merge Example */
data work.merged;
    merge sashelp.class sashelp.cars;
    by name;
run;
