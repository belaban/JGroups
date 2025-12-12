package org.jgroups.tests;

/**
 * Computes the monthly amount of repayment accoring to the French method given the principal, interest and duration.
 * @author Bela Ban
 * @since 5.5.3
 */
public class FrenchRepayment {
    protected static final int ANNUAL_PAYMENTS=12;

    protected static double frenchRepayment(int loan_amount, double interest, int loan_duration) {

        double i=interest/100;
        int m=ANNUAL_PAYMENTS;


        // Payment = (C*i/m)/(1-(1+i/m)EXP(-m*n))
        // C: outstanding principal on the loan.
        // i: annual nominal interest rate (expressed as a decimal).
        // m: number of annual payments.
        // n: number of years in the loan period.

        double interest_per_year=loan_amount*i/m;
        double divider=Math.pow(1.0+i/m, -m*loan_duration);
        divider=1.0 - divider;
        return interest_per_year / divider;
    }


    public static void main(String[] args) {
        if(args.length != 3) {
            System.err.println("bla2 <loan amount> <interest (e.g. 2.99)> <loan duration>");
            return;
        }

        int loan_amount=Integer.parseInt(args[0]);
        double interest=Double.parseDouble(args[1]), i=interest / 100;
        int loan_duration=Integer.parseInt(args[2]);

        double monthly_repayment=FrenchRepayment.frenchRepayment(loan_amount, interest, loan_duration);
        System.out.printf("\nloan amount: %,d\ninterest: %.2f\nload duration: %d years\n\nMonthly payment: %.2f\n",
                          loan_amount, interest, loan_duration, monthly_repayment);
    }
}
