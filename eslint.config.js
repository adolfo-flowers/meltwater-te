import js from "@eslint/js";
import prettierConfig from "eslint-config-prettier";
import globals from "globals";

export default [
    js.configs.recommended, // Base recommended rules
    prettierConfig,         // Turns off conflicting formatting rules
    {
        languageOptions: {
            ecmaVersion: "latest",
            sourceType: "module",
            globals: {
                ...globals.node,
            },
        },
        rules: {
            "no-unused-vars": "warn",
            "no-console": "off",
        },
    },
];
