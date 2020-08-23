
const x: unknown = undefined

if (x) {
    console.log(`simple - we have x`)
} else {
    console.log(`simple - NO x`)
}

// https://stackoverflow.com/questions/28975896/is-there-a-way-to-check-for-both-null-and-undefined
if (x != null) { // this is a better option that `if(x)`, if you can the if block to execute whenever x is false or empty string. ie, if you truely only want the code to NOT execute when x is undefined or null
    console.log(`check null - we have x`)
} else {
    console.log(`check null - NO x`)
}

if (!!x) { // this is the exact same thing as `if(x)`. so !! only necessary if converting an object to a true of false value. https://stackoverflow.com/questions/40182319/is-it-necessary-to-use-double-exclamation-points-in-order-to-make-sure-that
    console.log(`!! - we have x`)
} else {
    console.log(`!! - NO x`)
}
