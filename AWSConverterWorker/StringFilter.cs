using System;


namespace ConvertDocxToText1
{
    /*
     * this one will be used do to some string filtering 
     * it will have some static methods  that do filtering 
     */
    public class StringFilter
    {
        /*
         * string with numbers and some extra characters that apear close to numbers 
         */
        public static bool IsAlmostNuberOnlyString(string str)
        {
            bool isDigit = true;
            char singleQuote = @"'".ToCharArray()[0];
            for (int i = 0; i < str.Length; ++i)
            {
                char c = str[i];
                if (Char.IsDigit(c))
                    continue;
                if (Char.IsWhiteSpace(c))
                    continue;
                if (c == '%' || c == '+' || c == '-' || c == '.' || c == ',' || c == singleQuote)
                    continue;
                if (c == '<' || c == '>' || c == '=' || c == '(' || c == ')')
                    continue;

                isDigit = false;
                break;
            }

            return isDigit;
        }
        public static bool IsBlankLineOnly(string str)
        {
            for (int i = 0; i < str.Length; ++i)
            {
                char c = str[i];
                if (Char.IsWhiteSpace(c))
                    continue;
                return false;
            }
            return true;
        }
    }
}
