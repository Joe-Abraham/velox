# Velox Coding Style Guidelines for AI Assistants

This document provides comprehensive coding guidelines specifically for AI assistants working with the Velox codebase. Please follow these conventions strictly to maintain consistency and code quality.

## C++ Style

* Always ensure your code is clang-format compatible.
* If you're working on a legacy file which is not clang-format compliant yet, refrain from formatting the entire file in the same PR as it pollutes your original PR and makes it harder to review.
* Submit a separate diff/PR with the format-only changes.

## Naming Conventions

* Use **PascalCase** for types (classes, structs, enums, type aliases, type template parameters) and file names.
* Use **camelCase** for functions, member and local variables, and non-type template parameters.
* **camelCase_** for private and protected members variables.
* Use **snake_case** for namespace names and build targets
* Use **UPPER_SNAKE_CASE** for macros.
* Use **kPascalCase** for static constants and enumerators.
* Use **testing** prefix for test only class methods, e.g. obj.testingFoo().
  As much as possible, refrain from adding test methods, and test objects using their public APIs. Only use test methods in rare edge cases where this is not possible. For example, MemoryAllocator::testingSetFailureInjection() is used to inject various memory allocation failures to test error handling paths.
* Use the **debug** prefix for query configs that are intended for debugging purposes only. These configs may enable expensive checks or disable selective code paths, and are not recommended for use in production environments. For example, `debug_disable_expression_with_peeling` is used to disable peeling optimization employed in expression evaluation.

### Enhanced Naming Guidelines with const/constexpr

**Constants and Compile-time Values:**
* Always use **kPascalCase** for constants, preferring `constexpr` over `const` when possible:
  ```cpp
  // Preferred
  constexpr int32_t kMaxBufferSize = 1024;
  constexpr std::string_view kDefaultName = "velox";

  // Avoid
  const int MAX_BUFFER_SIZE = 1024;  // Wrong case, prefer constexpr
  static const char* DEFAULT_NAME = "velox";  // Use constexpr std::string_view
  ```

**Member Functions:**
* Always mark member functions as `const` when they don't modify object state
* Use `constexpr` for functions that can be evaluated at compile time:
  ```cpp
  // Good
  bool isEmpty() const { return size_ == 0; }  // const member function
  constexpr int32_t calculateArea(int32_t w, int32_t h) { return w * h; }

  // Avoid  
  bool isEmpty() { return size_ == 0; }  // Missing const
  ```

**Test Naming:**
* Test fixture classes: Use **PascalCase** ending with `Test`
* Test cases: Use **PascalCase** for both suite and case names
* Test methods: Use **testingMethodName()** format for test-only class methods
  ```cpp
  // Good
  class ExpressionEvaluatorTest : public ::testing::Test {};
  TEST_F(ExpressionEvaluatorTest, EvaluatesArithmeticCorrectly) {}

  // Avoid
  class expression_evaluator_test : public ::testing::Test {};  // Wrong case
  TEST_F(ExpressionEvaluatorTest, evaluates_arithmetic) {}  // Wrong case
  ```

## Comments

Some good practices about code comments:

* Optimize code for the reader, not the writer.
  * Velox is growing and starting to be used by multiple compute engines and teams; as a result, more time will be spent reading code than writing it.
* Overall goal: make the code more obvious and remove obscurity.
* Comments should capture information that was on the writer's mind, but **couldn't be represented as code.**
  * As such, refrain from adding obvious comments, e.g: simple getter/setter methods.
  * However, "obviousness" is in the reader's mind - if a reviewer says something is not obvious, then it is not obvious.
    * Consider that the audience is an experienced Software Engineer with a moderate knowledge of the codebase.

### What should be commented:
* Every File
* Every Class
* Every method that's not an obvious getter/setter
* Every member variable
  * Do not simply restate the variable name. Either add a comment explaining the semantic meaning of that variable or do not add a comment at all.
  * This is an anti-pattern:
    ```cpp
    // A simple counter.
    size_t count_{0};
    ```
* For functions with large bodies, a good practice is to group blocks of related code, and precede them with a blank line and a high-level comment on what the block does.

### Comment style:

* Header comment for source files:
  * All source files (.h and .cpp) should have a standard header in a large comment block at the top; this should include the standard copyright notice, the license header, and a brief file description. This comment block should use `/* */`
* For single line comments, always use `//` (with a space separator before the comment).
* Comments should be full english sentences, starting with a capital letter and ending with a period (.).
  * Use: `// True if this node only sorts a portion of the final result.`
  * Instead of: `// true if this node only sorts a portion of the final result`
* For multi-line comments:
  * Velox will follow the doxygen comment style.
  * For multi-line comments within actual code blocks (the ones which are not to be exposed as documentation, use `//` (double slashes).
  * For comments on headers for classes, functions, methods and member variables (the ones to be exposed as documentation), use `///` (three slashes) at the front of each line.
  * Don't use old-style `/* */` comments inside code or on top-level header comments. It adds two additional lines and makes headers more verbose than they need to be.
* Special comments:
  * Use this format when something needs to be fixed in the future: `// TODO: Description of what needs to be fixed.`
  * Include enough context in the comment itself to make clear what will be done, without requiring any references from outside the code.
  * Do not include the author's username. If required, this can always be retrieved from git blame.

## Asserts and CHECKs

* For assertions and other types of validation, use `VELOX_CHECK_*` macros
  * `VELOX_CHECK_*` will categorize the error to be an internal runtime error.
  * `VELOX_USER_CHECK_*` will categorize the error to be a user error.
* Use `VELOX_FAIL()` or `VELOX_USER_FAIL()` to inadvertently throw an exception:
  * `VELOX_FAIL("Illegal state");`
* Use `VELOX_UNREACHABLE()` when a particular branch/block should never be executed, such as in a switch statement with an invalid `default:` block.
* Use `VELOX_NYI()` for features or code paths that are not implemented yet.
* When comparing two values/expressions, prefer to use:
  * `VELOX_CHECK_LT(idx, children_.size());`
* Rather than:
  * `VELOX_CHECK(idx < children_.size());`
* The former will evaluate and include both expressions values in the exception.
* All macro also provide the following optional features:
  * Specifying error code (error code listed in `velox/common/base/VeloxException.h`):
    * `VELOX_CHECK_EQ(v1[, v2[, error_code]]);`
    * `VELOX_USER_CHECK_EQ(v1[, v2[, error_code]])`
  * Appending error message:
    * `VELOX_CHECK_EQ(v1, v2, "Some error message")`
    * `VELOX_USER_CHECK_EQ(v1, v2, "Some error message")`
  * Error message formatting via fmt:
    * `VELOX_USER_CHECK_EQ(v1, v2, "My complex error message {} and {}", str, i)`
    * Note that the values of v1 and v2 are already included in the exception message by default.

## Variables

* Do not declare more than one variable on a line (this helps accomplish the other guidelines, promotes commenting, and is good for tagging).
  * Obvious exception: for-loop initialization
* Initialize most variables at the time of declaration (unless they are class objects without default constructors).
  * Prefer using uniform-initialization to make initialization more consistent across types, e.g., prefer `size_t size{0};` over `size_t size = 0;`
* Declare your variables in the smallest scope possible.
* Declare your variables as close to the usage point as possible within the given scope.
* Don't group all your variables at the top of the scope -- this makes the code much harder to follow.
* If the variable or function parameter is a pointer or reference type, group the `*` or `&` with the type -- pointer-ness or reference-ness is an attribute of the type, not the name.
  * `int* foo;` `const Bar& bar;` NOT `int *foo;` `const Bar &bar`;
  * Beware that `int* foo, bar;` will be parsed as declaring `foo` as an `int*` and `bar` as an `int`. Note that multiple declaration is discouraged.

### For member variables:
* Group member variable and methods based on their visibility (public, protected and private)
  * It's ok to have multiple blocks for a given level.
* Most member variables should come with a short comment description and an empty line above that.
* Refrain from using public member variables whenever possible, in order to promote encapsulation.
  * Leverage getter/setter methods when appropriate.
  * Name getter methods after the variable, e.g: `foo()`, and setter methods using the "set" prefix, e.g: `setFoo()`
  * Always mark getter methods as const.
  * Consider using constexpr for simple getter methods that can be evaluated at compile time.
* Prefer to use value-types, `std::optional`, and `std::unique_ptr` in that order.
  * Value-types are conceptually the simplest and cheapest.
  * `std::optional` allows you to express "may be null" without the additional complexity of manual storage duration.
  * `std::unique_ptr<>` should be used for types that are not cheaply movable but need to transfer ownership, or which are too large to store on the stack. Note that most types that perform large allocations already store their bulk memory on-heap.

## Constants and Const Correctness

* Always use `nullptr` if you need a constant that represents a null pointer (`T*` for some `T`); use `0` otherwise for a zero value.
* For large literal numbers, use ' to make it more readable, e.g:  `1'000'000` instead of `1000000`.
* For floating point literals, never omit the initial 0 before the decimal point (always `0.5`, not `.5`).
* File level variables and constants should be defined in an anonymous namespace.
* Always prefer const variables and enum to using preprocessor (#define) to define constant values.
* Prefer `enum class` over `enum` for better type safety.
* As a general rule, do not use string literals without declaring a named constant for them.
  * The best way to make a constant string literal is to use constexpr `std::string_view`/`folly::StringPiece`
  * **NEVER** use `std::string` - this makes your code more prone to SIOF bugs.
  * Avoid `const char* const` and `const char*` - these are less efficient to convert to `std::string` later on in your program if you ever need to because `std::string_view`/`folly::StringPiece` knows its size and can use a more efficient constructor. `std::string_view`/`folly::StringPiece` also has richer interfaces and often works as a drop-in replacement to `std::string`.
  * Need compile-time string concatenation? You can use `folly::FixedString` for that.

### When to Use const vs constexpr

**Use constexpr for:**
* Compile-time constants that can be evaluated at compile time
* Functions that can be evaluated at compile time
* Static class constants
  ```cpp
  // Compile-time constants
  constexpr size_t kBufferSize = 4096;
  constexpr double kPi = 3.14159;

  // Static class constants  
  class MyClass {
    static constexpr int32_t kMaxElements = 1000;
  };

  // Compile-time functions
  constexpr int32_t square(int32_t x) { return x * x; }
  ```

**Use const for:**
* Runtime constants (values determined at runtime)
* Function parameters that should not be modified
* Member functions that don't modify object state
  ```cpp
  // Runtime constants
  const std::string filename = getConfigFileName();
  const auto& config = getConfiguration();

  // Const member functions
  size_t size() const { return size_; }
  ```

**Prefer static constexpr over static const:**
```cpp
// Good
static constexpr int32_t kDefaultSize = 100;

// Avoid
static const int32_t kDefaultSize = 100;  // Use constexpr when possible
```

## Macros

Do not use them unless absolutely necessary. Whenever possible, use normal inline functions or templates instead. If you absolutely need, remember that macro names are always upper-snake-case. Also:

* Use parentheses to sanitize complex inputs.
  * For example, `#define MUL(x, y) x * y` is incorrect for input like `MUL(2, 1 + 1)`. The author probably meant `#define MUL(x, y) ((x) * (y))` instead.
* When making macros that have multiple statements, use this idiom:
  * `do { stmt1; stmt2; } while (0)`
  * This allows this block to act the most like a single statement, usable in if/for/while even if they don't use braces and forces use of a trailing semicolon.

## Headers and Includes

* All header files must have a single-inclusion guard using `#pragma once`
* Included files and using declarations should all be at the top of the file, and ordered properly to make it easier to see what is included.
  * Use clang-format to order your include and using directives.
* Includes should always use the full path (relative to github's root dir).
* Whenever possible, try to forward-declare as much as possible in the .h and only `#include` things you need the full implementation for.
  * For instance, if you just use a `Class*` or `Class&` in a header file, forward-declare `Class` instead of including the full header to minimize header dependencies.
* Put small private classes that will only get used in one place into a .cpp, inside an anonymous namespace. A common example is a custom comparator class.
* Be aware of what goes into your .h files. If possible, try to separate them into "Public API" .h files that may be included by external modules. These public .h files should contain only those functions or classes that external users need to access.
* No large blocks of code should be in .h files that are widely included if these blocks aren't vital to all users' understanding of the interface the .h represents. Split these large implementation blocks into a separate `-inl.h` file.
  * `-inl.h` files should exist only to aid readability with the expectation that a user of your library should only need to read the .h to understand the interface. Only if someone needs to understand your implementation should opening the `-inl.h` be necessary (or the .cpp file, for that matter).

## Function Arguments

### Const
* All objects, whenever possible, should be passed to functions as const-refs (`const T&`). This makes APIs much clearer as to whether it's an input or output arg, reduces the need for NULL-checks (and reduces crashing bugs), and helps enforce good encapsulation.
  * An obvious exception is objects that are trivially copy- or move-constructible, like primitive types or `std::unique_ptr`.
  * Don't pass by const reference when passing by value is both correct and cheaper.
* All non-static member functions should be marked as const if calling them doesn't change the behavior/response of any member.

### References as function arguments
* For input or read-only parameters, pass by `const&`
* For nullable or optional parameters, use higher order primitives like `std::optional`; raw pointers are also appropriate.
* For output, mutable, or in/out parameters:
  * Non-const ref is the recommendation if the argument is not nullable.
  * If it's nullable, use a raw pointer.
* Always use `std::optional` instead of `folly::Optional` and `boost::optional`.

### String parameters
* Prefer `std::string_view` to `const std::string&`
  * This avoids unnecessary construction of a std::string when passing other types. This often happens when string literals are passed into functions.
  * If you need a `std::string` inside the function, however, take a `std::string` to move the copy to the API boundary. This allows the caller to avoid the copy with a move.

### R-value references
* Almost never take an r-value reference as a function argument; take the argument by value instead. This usually as efficient as taking an r-value reference, but simpler and more flexible at the call site. For example:
  ```cpp
  InPredicate::InPredicate(folly::F14HashSet<T>&& rhs) : rhs_(std::move(rhs)) {}
  ```
  * is more complex than the version that takes expr by value:
  ```cpp
  InPredicate::InPredicate(folly::F14HashSet<T> rhs) : rhs_(std::move(rhs)) {}
  ```
  * However, the first requires either a std::move() or an explicit copy at the call point; the second doesn't. The performance of the two should be almost identical.

### Function call clarity
* Add comments when it's not clear which argument is intended in a function call. This is particularly a problem for longer signatures with repeated types or constant arguments. For example, `phrobinicate(/*elements=*/{1, 2}, /*startOffset=*/0, /*length=*/2)`
* Use the /*argName=*/value format (note the lack of spaces). Clang-tidy supports checking the given argument name against the declaration when this format is used.

## Namespaces

* All Velox code should be placed inside the `facebook::velox` namespace
* Always use nested namespace definition:
  * `namespace facebook::velox::core {` and not
  * `namespace facebook { namespace velox { namespace core {`
* Always add an inline comment at the end of the namespace definition, and surround it by empty lines:

```cpp
namespace facebook::velox::exec {

myFunc();

} // namespace facebook::velox::exec
```

  * not:

```cpp
namespace facebook::velox::exec {
myFunc();
}
```

* Use sub namespaces (e.g: facebook::velox::core) to logically group large chunks of related code and prevent identifier clashes.
  * Namespaces should make it easier to code, not harder. Refrain from creating hierarchies that are way too long.
  * Namespace don't necessarily need to reflect the on-disk layout.
    * This isn't java.

### Guidelines for importing namespaces:
* Don't EVER put `using namespace anything;` OR `using anything::anything;` in a header file. This pollutes the global namespace and makes it extremely difficult to refactor code.
* It's very useful to not have to fully-qualify types as it can make code more readable. In header files, it's unavoidable. ALWAYS fully qualify names in a header (e.g. it's `std::string`, not just `string`).
* In cpp files, best practice is to add a using declaration after your list of `#includes` (e.g. `using tests::VectorMaker;`) and to then just write `VectorMaker`.
* `using namespace std;` can lead to surprising behavior, especially when other modules don't follow best practices with their using statements and what they dump into global scope. The best defensive programming practice for std it to fully qualify symbols, e.g: `std::string`, std::vector
  * Do not use `using namespace std;`
* Use `using namespace anything_else;` somewhat sparingly – the whole point of namespaces is obliterated when the code starts doing this prolifically. Frequently, `using foo::bar::BazClass;` is better.

## Type Aliases

* For types widely used together with std::shared_ptr, consider introducing aliases for std::shared_ptr<Xxx> using naming convention XxxPtr. In some cases it makes sense to alias std::shared_ptr<const Xxx>. Here are some examples of existing aliases: TypePtr, VectorPtr, FlatVectorPtr, ArrayVectorPtr, MapVectorPtr, RowVectorPtr, TypedExprPtr, PlanNodePtr.

```cpp
using TypePtr = std::shared_ptr<const Type>;
```
* Similarly, widely used template types also benefit from shorter or clearer aliases.

```cpp
using ContinueFuture = folly::SemiFuture<bool>;
using ContinuePromise = VeloxPromise<bool>;
```

## Automated Code Quality Checks

The GitHub Actions workflow automatically checks for:

1. **Variable naming**: camelCase for variables, kPascalCase for constants
2. **Function naming**: camelCase for functions, const correctness for member functions  
3. **Test naming**: PascalCase for test cases and fixtures
4. **Const usage**: Prefer constexpr over const for compile-time constants
5. **Static constants**: Use static constexpr instead of static const
6. **Member function const-ness**: Mark non-mutating functions as const

**Common violations to avoid:**
```cpp
// ❌ Bad examples
const int MAX_SIZE = 100;           // Use: constexpr int32_t kMaxSize = 100;
static const char* FILE_NAME;       // Use: static constexpr std::string_view kFileName;
void ProcessData() {}               // Use: void processData() {}
bool is_empty() { return true; }    // Use: bool isEmpty() const { return true; }
TEST(mytest, test_case) {}          // Use: TEST(MyTest, TestCase) {}

// ✅ Good examples  
constexpr int32_t kMaxSize = 100;
static constexpr std::string_view kFileName = "data.txt";
void processData() {}
bool isEmpty() const { return true; }
TEST(MyTest, TestCase) {}
```

These automated checks help maintain code quality and consistency across the Velox codebase.

## Key Principles for AI Assistants

When working with the Velox codebase:

1. **Prioritize readability**: Code is read more often than it's written
2. **Follow existing patterns**: Look at surrounding code and maintain consistency
3. **Use appropriate abstractions**: Prefer VELOX_CHECK_* over assert, constexpr over const when possible
4. **Document non-obvious code**: Every public API should be documented
5. **Maintain const correctness**: Mark functions const when they don't modify state
6. **Use modern C++ features**: Prefer enum class, constexpr, std::optional, etc.
7. **Keep functions focused**: Large functions should be broken down with clear comments
8. **Minimize header dependencies**: Use forward declarations when possible
