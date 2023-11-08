##
# Practical Course High-Performance Query Processing
# Makefile
#

CC					= gcc
CXX					= g++
RM					= rm -f

INCLUDE_FLAGS	   := -Iinclude
DEBUG_FLAGS		   := -fsanitize=undefined
SANITIZE_FLAGS	   := -fno-omit-frame-pointer -fsanitize=address
RELEASE_FLAGS	   := -DNDEBUG
CXXFLAGS		   := -Wall -std=c++20 $(INCLUDE_FLAGS) -g $(CXXFLAGS) -lfmt -ltbb -pthread

BINDIR				= bin
SRCDIR				= tasks
RSRCDIR				= resources

## set target to debug if unspecified
ifeq ($(target),)
	target := debug
endif


## select flags based on target
ifeq ($(target),release)
	CXXFLAGS += $(RELEASE_FLAGS) -O3
else ifeq ($(target),staging)
	CXXFLAGS += $(DEBUG_FLAGS) -O3
else ifeq ($(target),sanitize)
	CXXFLAGS += $(SANITIZE_FLAGS) $(DEBUG_FLAGS) -O0
else
	CXXFLAGS += $(DEBUG_FLAGS) -O0
endif

## rebuild w/o file changes if target changed
TARGET_MARKER      := $(BINDIR)/.$(target).target
$(TARGET_MARKER):
	mkdir -p $(BINDIR)
	$(RM) $(BINDIR)/.*.target
	touch $(TARGET_MARKER)

## build targets
$(BINDIR)/%.out: $(TARGET_MARKER) $(SRCDIR)/%.cpp
	$(CXX) $(patsubst $(BINDIR)/%.out, $(SRCDIR)/%.cpp, $@) $(CXXFLAGS) -o $@


## if quiet flag is set, don't ouptput generated code
CODEGEN_COMMAND := $(if $(quiet),clang-format --style=WebKit >, clang-format --style=WebKit | tee)

get_task = $(basename $(word 3,$(subst -, ,$1)))

VARIANT_FLAG = -DVARIANT_$(variant)

## special rules for query compiler
$(BINDIR)/p2c-task-%.out: $(TARGET_MARKER) $(SRCDIR)/p2c-task-%.cpp $(RSRCDIR)/queryFrame.cpp $(SRCDIR)/additionaloperators.hpp
	$(CXX) -std=c++20 $(patsubst $(BINDIR)/p2c-task-%.out, $(SRCDIR)/p2c-task-%.cpp, $@) $(CXXFLAGS) $(VARIANT_FLAG) -O0 -g -o $(subst task,tmp,$@)
	cp $(RSRCDIR)/queryFrame.cpp $(BINDIR)/queryFrame.cpp
	$(subst task,tmp,$@) | $(CODEGEN_COMMAND) $(BINDIR)/p2c-query.cpp
	$(CXX) -std=c++20 $(BINDIR)/queryFrame.cpp $(CXXFLAGS) -DTASK_NAME="$(call get_task,$@)" $(VARIANT_FLAG) -o $@
clean:
	$(RM) -r $(BINDIR)/*

.PHONY: clean

# end
