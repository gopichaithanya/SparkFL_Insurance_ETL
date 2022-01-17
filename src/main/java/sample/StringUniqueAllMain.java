package sample;

public class StringUniqueAllMain {
         public static void main(String[] args) {

            System.out.println("java2blog has all unique chars : "+ hasAllUniqueChars("java2blog"));
            System.out.println("Apple has all unique chars : "+ hasAllUniqueChars("apple"));
            System.out.println("index has all unique chars : "+ hasAllUniqueChars("index"));
            System.out.println("world has all unique chars : "+ hasAllUniqueChars("world"));
        }

        public static boolean hasAllUniqueChars (String word) {

            for(int index=0;index < word.length(); index ++)   {

                char c =word.charAt(index);
                if(word.indexOf(c)!=word.lastIndexOf(c))
                    return false;
            }

            return true;
        }
    }

