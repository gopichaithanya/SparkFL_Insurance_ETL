package sample;

public class StringReverse {

    public static void main(String [] args){
        String blogName="JavaWorks";
        String reverse = "";
        for(int i = blogName.length()-1;i>=0;i--)
            reverse =reverse+blogName.charAt(i);
         System.out.println("reverse of a String:"+reverse);
    }
}
